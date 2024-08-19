"""REST client handling, including stripeStream base class."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, cast

import requests
from requests.exceptions import JSONDecodeError
from memoization import cached
from backports.cached_property import cached_property
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from pendulum import parse
from typing import Any, Callable, Dict, Iterable, Optional
import backoff
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError

import singer
from singer import StateMessage

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
    invoice_lines = []
    expand = []
    fullsync_ids = []
    get_data_from_id = False
    lines_field = "lines"

    @cached
    def get_starting_time(self, context):
        start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    @property
    def last_id_jsonpath(self):
        jsonpath = self.records_jsonpath.replace("*", "-1")
        return f"{jsonpath}.id"

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object."""
        return BearerTokenAuthenticator.create_for_stream(
            self, token=self.config.get("access_token") or self.config.get("client_secret")
        )

    @property
    def http_headers(self) -> dict:
        """Return headers dict to be used for HTTP requests."""
        result = self._http_headers
        result["Stripe-Version"] = "2022-11-15"
        if self.config.get("account_id"):
            result["Stripe-Account"] = self.config.get("account_id")
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
        if self.replication_key and self.path != "credit_notes":
            start_date = self.get_starting_time(context)
            params["created[gt]"] = int(start_date.timestamp())
        if self.path == "events" and self.event_filter:
            params["type"] = self.event_filter
        if not self.get_from_events and self.expand:
            params["expand[]"] = self.expand
        return params

    @property
    def get_from_events(self):
        state_date = self.get_starting_time({}).replace(tzinfo=None)
        start_date = parse(self.config.get("start_date")).replace(tzinfo=None)
        return state_date != start_date

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
    
    @property
    def not_sync_invoice_status(self):
        not_sync_invoice_status = self.config.get("inc_sync_ignore_invoice_status")
        if not_sync_invoice_status:
            return not_sync_invoice_status.split(",")
        return ["deleted"]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        decorated_request = self.request_decorator(self._request)
        base_url = "/".join(self.url_base.split("/")[:-2])
        try:
            records = extract_jsonpath(self.records_jsonpath, input=response.json())
        except:
            records = response

        if self.name == "plans" and self.path == "events":
            records = list(records)
            # for plans get all prices, including the updated ones from subscriptions
            plans = [plan for plan in records if plan["type"].startswith("plan")]
            # Extract plans from the subscriptions
            subscription_plans = []
            [subscription_plans.extend(item.get("data", {}).get("object", {}).get("items", {}).get("data", [])) for item in records if (item["type"] == "customer.subscription.updated")]
            subscription_plans = [item["plan"] for item in subscription_plans]

            invoice_plans = []
            [invoice_plans.extend(item.get("data", {}).get("object", {}).get("lines", {}).get("data", [])) for item in records if (item["type"].startswith("invoice"))]
            invoice_plans = [item.get("price") or item.get("plan") for item in invoice_plans if item.get("price") is not None or item.get("plan") is not None]

            # Combine both sets of plans
            records = plans + subscription_plans + invoice_plans
        if self.name == "products" and self.path == "events":
            records = list(records)
            # for products get all products, including the updated ones from invoiceitems
            products = [product.get("data", {}).get("object") for product in records if product["type"].startswith("product")]
            # Extract plans from the subscriptions
            invoiceitems_products = []
            [invoiceitems_products.extend(item.get("data", {}).get("object", {}).get("lines", {}).get("data", [])) for item in records if item["type"].startswith("invoice")]
            invoiceitems_products = [item.get("price") or item.get("plan") for item in invoiceitems_products if item.get("price") is not None or item.get("plan") is not None]
            # get product ids
            [item.update({"id": item["product"]}) for item in invoiceitems_products]
            # Combine both sets of plans
            records = products + invoiceitems_products

        for record in records:
            # logic for incremental syncs
            if self.name != "events" and ((self.path == "events" and self.get_from_events) or self.get_data_from_id): 
                event_date = record["created"]
                if self.name not in ["plans", "products"]:
                    record = record["data"]["object"]
                record_id = record.get("id")
                if (
                    not record_id
                    or (record_id in self.event_ids)
                    or (
                        self.object != record["object"]
                        if self.object not in ["plan", "product"]
                        else False
                    )
                ):
                    continue

                # filter status that we need to ignore, ignore deleted status as default
                if record.get("status") in self.not_sync_invoice_status:
                    self.logger.debug(f"{self.name} with id {record_id} skipped due to status {record.get('status')}")
                    continue
                # using prices API instead of plans API
                if self.name == "plans":
                    url = base_url + f"/v1/prices/{record_id}"
                # discounts is a synthetic stream, it uses data from invoices
                elif self.name == "discounts":
                    url = base_url + f"/v1/invoices/{record_id}"
                else:
                    url = base_url + f"/v1/{self.name}/{record['id']}"
                params = {}
                if self.expand:
                    params["expand[]"] = self.expand

                response_obj = decorated_request(
                    self.prepare_request_lines(url, params), {}
                )
                if response_obj.status_code in self.ignore_statuscode:
                    self.logger.debug(f"{self.name} with id {record_id} skipped")
                    continue
                record = response_obj.json()
                record["updated"] = event_date

                # add record id to event_ids to not get dupplicates in incremental syncs
                self.event_ids.append(record_id)
            if not record.get("updated") and "created" in record:
                record["updated"] = record["created"]
            
            # iterate through lines pages
            if self.lines_field in record:
                if record[self.lines_field].get("has_more"):
                    next_page_token = self.get_next_page_token_lines(record[self.lines_field])
                    url = base_url + record[self.lines_field]["url"]
                    lines = record[self.lines_field].get("data", [])
                    while next_page_token:
                        params = {"limit": 100, "starting_after": next_page_token}
                        lines_response = decorated_request(
                            self.prepare_request_lines(url, params), {}
                        )
                        response_obj = lines_response.json()
                        next_page_token = self.get_next_page_token_lines(response_obj)
                        response_data = response_obj.get("data", [])
                        lines.extend(response_data)
                    record[self.lines_field]["data"] = lines
                    record[self.lines_field]["has_more"] = False
            
            # clean dupplicates for fullsync streams that fetch data from more than one endpoint
            if hasattr(self, "from_invoice_items"):
                if self.from_invoice_items:
                    if record["id"] in self.fullsync_ids:
                        continue
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


    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        try:
            response_content = response.json()
    
            if response_content.get("error"):
                error = response_content.get("error")
                return f'Error: {error.get("message")} at path {self.path}'
        except JSONDecodeError:
            # ignore JSON errors
            pass

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.text} for path: {self.path}"
        )

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

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(StateMessage(value=tap_state))


class StripeStreamV2(stripeStream):
    """Class for the streams that need to get data from invoice items in full sync and more than one event in incremental syncs """

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # activate flag if it's a full sync to fetch prices from invoiceitems
        if not self.stream_state.get("replication_key"):
            self.from_invoice_items = True
        return super().request_records(context)

    def get_next_page_token(self, response, previous_token):
        next_page_token = super().get_next_page_token(response, previous_token)
        # get a dummy next page token to iterate first through invoice_items and then through prices
        if self.from_invoice_items and not next_page_token:
            next_page_token = 1
            self.from_invoice_items = False
        return next_page_token
    
    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        # delete the dummy next page token to avoid errors
        if not self.from_invoice_items and next_page_token == 1:
            del params["starting_after"]
        return params
