"""REST client handling, including stripeStream base class."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, cast

import requests
from memoization import cached
from backports.cached_property import cached_property
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse
from typing import Any, Callable, Dict, Iterable, Optional
import backoff
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError



class stripeStream(RESTStream):
    """stripe stream class."""

    url_base = "https://api.stripe.com/v1/"
    _page_size = 100

    records_jsonpath = "$.data[*]"
    primary_keys = ["id"]
    event_filter = None
    event_ids = []
    ignore_statuscode = [404]
    params = {}

    @cached
    def get_starting_time(self, context):
        start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def expand(self,second_request=False):
        return None

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
        result["Stripe-Version"] = "2022-11-15"
        return result

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_more = (response.json() or {}).get("has_more")
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
            start_date = self.get_starting_time(context)
            params["created[gt]"] = int(start_date.timestamp())
        if self.path=="events" and self.event_filter:
            params["type"] = self.event_filter
        
        expansion = self.expand(second_request=False)
        if not self.get_from_events and expansion:
           params["expand[]"] = expansion

        return params

    @property
    def get_from_events(self):
        state_date = self.get_starting_time({}).replace(tzinfo=None)
        start_date = parse(self.config.get("start_date")).replace(tzinfo=None)
        return state_date!=start_date

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
        decorated_request = self.request_decorator(self._request)
        base_url = "/".join(self.url_base.split("/")[:-2])
        for record in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if self.path=="events" and self.event_filter:
                event_date = record["created"]
                record = record["data"]["object"]
                record_id = record.get("id")
                if not record_id or (record_id in self.event_ids) or (self.object!=record["object"]):
                    continue
                # when the invoice is deleted or draft
                if record.get("status") in ["deleted"]:
                    self.logger.debug(f"{self.name} with id {record_id} skipped due to status {record.get('status')}")
                    continue
                elif record.get("status") in ['draft']:
                    url = base_url + f"/v1/{self.name}/upcoming?"
                else: 
                    url = base_url + f"/v1/{self.name}/{record['id']}" 

                params = {}
                if self.expand(second_request=True):
                    params["expand[]"] = self.expand(second_request=True)
                
                if record.get("status") in ['draft']:
                    params['customer'] = record.get('customer')
            
                response_obj = decorated_request(self.prepare_request_lines(url,params), {})
                if response_obj.status_code in self.ignore_statuscode:
                    self.logger.debug(f"{self.name} with id {record_id} skipped")
                    continue
                record = response_obj.json()
                record["updated"] = event_date
                self.event_ids.append(record_id)
            if not record.get("updated") and "created" in record:
                record["updated"] = record["created"]
            if "lines" in record:
                if record["lines"].get("has_more"):
                    next_page_token = self.get_next_page_token_lines(record["lines"])
                    url = base_url + record["lines"]["url"]
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
    

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException,
                ConnectionError,
            ),
            max_tries=5,
            factor=2,
        )(func)
        return decorator
    
    def validate_response(self, response: requests.Response) -> None:
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500 and response.status_code not in self.ignore_statuscode:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)
