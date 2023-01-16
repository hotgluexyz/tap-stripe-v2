"""REST client handling, including stripeStream base class."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, cast

import requests
from backports.cached_property import cached_property
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse


class stripeStream(RESTStream):
    """stripe stream class."""

    url_base = "https://api.stripe.com/v1/"
    _page_size = 100

    records_jsonpath = "$.data[*]"
    primary_keys = ["id"]
    event_filter = None
    event_ids = []

    params = {}

    @property
    def last_id_jsonpath(self):
        jsonpath = self.records_jsonpath.replace("*", "-1")
        return f"{jsonpath}.id"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self, token=self.config.get("client_secret")
        )

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = self._http_headers
        result["Stripe-Version"] = "2020-08-27"
        return result

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_more = extract_jsonpath("$.has_more", response.json())
        if has_more:
            return next(extract_jsonpath(self.last_id_jsonpath, response.json()), None)
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = self.params.copy()
        params["limit"] = self._page_size
        if next_page_token:
            params["starting_after"] = next_page_token
        if self.replication_key and self.path!="credit_notes":
            start_date = self.get_starting_timestamp(context)
            params["created[gt]"] = int(start_date.timestamp())
        if self.path=="events" and self.event_filter:
            params["type"] = self.event_filter
        return params

    @property
    def get_from_events(self):
        start_date = self.get_starting_timestamp({}).replace(tzinfo=None)
        return start_date!=parse(self.config.get("start_date"))

    @cached_property
    def datetime_fields(self):
        datetime_fields = []
        for key, value in self.schema["properties"].items():
            if value.get("format") == "date-time":
                datetime_fields.append(key)
        return datetime_fields

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for field in self.datetime_fields:
            if row.get(field):
                dt_field = datetime.utcfromtimestamp(int(row[field]))
                row[field] = dt_field.isoformat()
        return row

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        for record in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if self.path=="events" and self.event_filter:
                event_date = record["created"]
                record = record["data"]["object"]
                record["updated"] = event_date
                record_id = record.get("id")
                if (not record_id) or (record_id in self.event_ids) or (self.object!=record["object"]):
                    continue
                self.event_ids.append(record_id)
            if not record.get("updated"):
                record["updated"] = record["created"]
            if "lines" in record:
                if record["lines"].get("has_more"):
                    next_page_token = self.get_next_page_token_lines(record["lines"])
                    base_url = "/".join(self.url_base.split("/")[:-2])
                    url = base_url + record["lines"]["url"]
                    decorated_request = self.request_decorator(self._request)
                    lines = record["lines"].get("data", [])
                    while next_page_token:
                        params = {"limit": 100, "starting_after": next_page_token}
                        lines_response = decorated_request(
                            self.prepare_request_lines(url, params), {}
                        )
                        next_page_token = self.get_next_page_token_lines(lines_response)
                        response_obj = lines_response.json()
                        response_data = response_obj.get("data", [])
                        lines.extend(response_data)
                    record["lines"]["data"] = lines
                    record["lines"]["has_more"] = False
            yield record

    def get_next_page_token_lines(self, response: requests.Response) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_more = extract_jsonpath("$.has_more", response)
        if has_more:
            return next(extract_jsonpath(self.last_id_jsonpath, response), None)
        return None

    def prepare_request_lines(self, url, params) -> requests.PreparedRequest:
        http_method = self.rest_method
        headers = self.http_headers
        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                ),
            ),
        )
        return request
