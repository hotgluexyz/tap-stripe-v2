"""REST client handling, including stripeStream base class."""

from datetime import datetime
from typing import Any, Dict, Iterable, Optional, cast, List

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
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError, InvalidStreamSortException
import copy
import concurrent.futures
import math

import singer
from singer import StateMessage
import requests
from requests.adapters import HTTPAdapter
import time
import queue
import psutil
import os
import random
import threading
from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error,
)

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

    def log_memory_usage(self, tag=""):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / (1024 * 1024)  # In MB
        self.logger.info(f"[MEMORY] {tag}: {mem:.2f} MB")

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
        if not self.parent_stream_type:
            params["limit"] = self._page_size
        if next_page_token:
            params["starting_after"] = next_page_token
        if self.replication_key and self.path != "credit_notes":
            start_date = self.get_starting_time(context)
            params["created[gte]"] = int(start_date.timestamp())
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
        # add updated as created if not in record (normally for full syncs, for incremental updated is added from events)
        if not "updated" in row:
            row["updated"] = row.get("created")
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
    
    def clean_records_from_events(self, records):
        """
        Clean list of records fetched from events for incremental syncs 
        """
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
            
        # clean duplicates
        if self.name != "events" and ((self.path == "events" and self.get_from_events) or self.get_data_from_id): 
            clean_records = []
            for record in records:
                event_created_date = record["created"]
                if self.name not in ["plans", "products"]:
                    record = record["data"]["object"]
                record_id = record.get("id")
                if record_id not in self.event_ids:
                    self.event_ids.append(record_id)
                    record["created"] = event_created_date
                    clean_records.append(record)
            return clean_records
        else:
            return records
    
    def get_lines(self, record, decorated_request):
        base_url = self.url_base
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
                    response_obj = lines_response.json()
                    next_page_token = self.get_next_page_token_lines(response_obj)
                    response_data = response_obj.get("data", [])
                    lines.extend(response_data)
                record["lines"]["data"] = lines
                record["lines"]["has_more"] = False
        return record
    
    def get_record_from_events(self, record, decorated_request):
        # logic for incremental syncs
        base_url = self.url_base
        if self.name != "events" and ((self.path == "events" and self.get_from_events) or self.get_data_from_id):
            event_date = record["created"]
            record_id = record.get("id")
            if (
                not record_id
                or (
                    self.object != record["object"]
                    if self.object not in ["plan", "product"]
                    else False
                )
            ):
                return

            # filter status that we need to ignore, ignore deleted status as default
            if record.get("status") in self.not_sync_invoice_status:
                self.logger.debug(f"{self.name} with id {record_id} skipped due to status {record.get('status')}")
                return
            # using prices API instead of plans API
            if self.name == "plans":
                url = base_url + f"prices/{record_id}"
            # discounts is a synthetic stream, it uses data from invoices
            elif self.name == "discounts":
                url = base_url + f"invoices/{record_id}"
            else:
                url = base_url + f"{self.name}/{record['id']}"
            params = {}
            if self.expand:
                params["expand[]"] = self.expand

            response_obj = decorated_request(
                self.prepare_request_lines(url, params), {}
            )
            if response_obj.status_code in self.ignore_statuscode:
                self.logger.debug(f"{self.name} with id {record_id} skipped")
                return
            record = response_obj.json()
            record["updated"] = event_date
        
        # iterate through lines pages
        record = self.get_lines(record, decorated_request)
        return record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        decorated_request = self.request_decorator(self._request)
        try:
            records = extract_jsonpath(self.records_jsonpath, input=response.json())
        except:
            records = response
        records = self.clean_records_from_events(records)
        yield from [self.get_record_from_events(record, decorated_request) for record in records]

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
            max_tries=10,
            factor=2,
            base=10,
            max_value=300
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
        raise RetriableAPIError("429")
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
                    tap_state["bookmarks"][stream_name]["partitions"] = []

        singer.write_message(StateMessage(value=tap_state))

class ConcurrentStream(stripeStream):
    """Class for the streams that need to get data from invoice items in full sync and more than one event in incremental syncs """

    def __init__(
        self,
        tap,
        name = None,
        schema = None,
        path = None,
    ) -> None:
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self.requests_session = requests.Session()
        adapter = HTTPAdapter(pool_connections=90, pool_maxsize=90)
        self.requests_session.mount("https://", adapter)

    queue_size = 2000

    @property
    def max_concurrent_requests(self):
        # use max from config for testing purposes (vcr don't work well with concurrency)
        if self.config.get("max_concurrent_requests"):
            return self.config["max_concurrent_requests"]
        # if stream has child streams selected use half of possible connections 
        # for parent and half for child to be able to do concurrent calls in both
        max_requests = 80 if "live" in self.config.get("client_secret") else 20 # using 80% of rate limit
        has_child_selected = any(getattr(obj, 'selected', False) for obj in self.child_streams)
        if has_child_selected:
            max_requests = max_requests/2
        return math.floor(max_requests)
    
    @property
    def requests_session(self) -> requests.Session:
        if not self._requests_session:
            self._requests_session = requests.Session()
        return self._requests_session
    
    @requests_session.setter
    def requests_session(self, value):
        self._requests_session = value

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        if context and "concurrent_params" in context:
            params.update(context["concurrent_params"])
        return params
    
    def safe_put(self, q: queue.Queue, item):
        while True:
            try:
                q.put(item, timeout=1)
                break
            except queue.Full:
                continue
    
    def concurrent_request(self, context, record_queue):
        error = None
        try:
            next_page_token: Any = None
            finished = False
            decorated_request = self.request_decorator(self._request)

            start_date = datetime.fromtimestamp(context["concurrent_params"]["created[gte]"]).isoformat()
            end_date = datetime.fromtimestamp(context["concurrent_params"].get("created[lt]", datetime.utcnow().timestamp())).isoformat()
            self.logger.info(f"Fetching data concurrently from {self.name} from {start_date} to {end_date}")

            while not finished:
                prepared_request = self.prepare_request(
                    context, next_page_token=next_page_token
                )
                resp = decorated_request(prepared_request, context)

                for record in self.parse_response(resp):
                    self.safe_put(record_queue, record)

                previous_token = copy.deepcopy(next_page_token)
                next_page_token = self.get_next_page_token(
                    response=resp, previous_token=previous_token
                )
                if next_page_token and next_page_token == previous_token:
                    raise RuntimeError(
                        f"Loop detected in pagination. "
                        f"Pagination token {next_page_token} is identical to prior token."
                    )
                # Cycle until get_next_page_token() no longer returns a value
                finished = not next_page_token
        except Exception as e:
            self.logger.exception(e)
            try:
                record_queue.put(("ERROR", str(e)), timeout=3)
            except queue.Full:
                error = e
        finally:
            if error is not None:
                raise error
    
    def get_concurrent_params(self, context, max_requests):
        start_date = self.get_starting_time(context)
        start_date_timestamp = int(start_date.timestamp())
        end_date_timestamp = int(datetime.utcnow().timestamp())

        current_start = start_date_timestamp
        max_size = 2592000 # one month as maximum
        min_size = 86400 # one day as minimum
        partition_size = math.ceil((end_date_timestamp - start_date_timestamp) / max_requests)
        partition_size = max_size if partition_size > max_size else min_size if partition_size < min_size else partition_size

        concurrent_params = []

        chunks = math.ceil((end_date_timestamp - start_date_timestamp)/partition_size)
        for i in range(chunks):
            params = {}
            current_end = current_start + partition_size
            params["created[gte]"] = current_start
            # don't put an end_date at the final chunk
            if i < chunks-1:
                params["created[lt]"] = current_end
            concurrent_params.append({"concurrent_params": params})
            current_start = current_end
        
        return concurrent_params
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        max_requests = 1
        requests_params = []

        # use concurrent requests for fullsyncs and streams that don't use events for incremental syncs
        if (not self.stream_state.get("replication_key_value") or self.name in [
            "invoice_items",
            "events",
            "subscription_schedules",
            "tax_rates",
            "balance_transactions",
            "products",
            "plans",
        ]) and not self.parent_stream_type:
            max_requests = self.max_concurrent_requests
            requests_params = self.get_concurrent_params(context, max_requests)

            # Time per batch to stay within the rate limit
            min_time_per_batch = 1 # The limit we use for concurrency is the same limit of requests allowed per second 

            for i in range(0, len(requests_params), max_requests):
                req_params = requests_params[i: i + max_requests]
                batch_start_time = time.time()

                record_queue = queue.Queue(self.queue_size)

                with concurrent.futures.ThreadPoolExecutor(max_workers=max_requests) as executor:
                    futures = []
                    for context in req_params:
                        future = executor.submit(self.concurrent_request, context, record_queue)
                        futures.append(future)

                    # Consumer loop for this batch
                    while not all(f.done() for f in futures):
                        # Check futures for errors first
                        for future in futures:
                            if future.done():
                                try:
                                    future.result()  # This will raise any exceptions from the thread, including parse_response errors
                                except Exception as e:
                                    self.logger.exception(f"Error in worker thread: {str(e)}")
                                    raise Exception(f"Worker thread error: {str(e)}")

                        try:
                            record = record_queue.get(timeout=5)  # 5 second timeout to allow checking futures
                            if isinstance(record, tuple) and record[0] == "ERROR":
                                # If an error is encountered, cancel all pending futures and drain the queue
                                self.logger.exception(f"Error from thread: {record[1]}")
                                for f in futures:
                                    f.cancel()
                                while not record_queue.empty():
                                    try:
                                        record_queue.get_nowait()
                                    except queue.Empty:
                                        break
                                raise Exception(f"Thread error: {record[1]}")
                            else:
                                yield record
                        except queue.Empty:
                            continue  # Continue checking futures if queue is empty

                # Enforce rate limit
                elapsed = time.time() - batch_start_time
                if elapsed < min_time_per_batch:
                    sleep_time = min_time_per_batch - elapsed
                    self.logger.info(f"Pausing for {sleep_time} to respect rate limits per second.")                    
                    time.sleep(min_time_per_batch - elapsed)  
                batch_start_time = time.time()    
        else:
            yield from super().request_records(context)

    def get_inc_concurrent_params(self, records, decorated_request):
        params = []
        for record in records:
            params.append({"record": record, "decorated_request": decorated_request})
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        # yield from [{"id": "hdhd"}]
        decorated_request = self.request_decorator(self._request)
        try:
            records = extract_jsonpath(self.records_jsonpath, input=response.json())
        except:
            records = response
        
        if self.name != "events" and ((self.path == "events" and self.get_from_events) or self.get_data_from_id):
            decorated_request = self.request_decorator(self._request)
            records = self.clean_records_from_events(records)
            # get records from "base_url/record_id" concurrently
            requests_params = self.get_inc_concurrent_params(records, decorated_request)
            max_requests = self.max_concurrent_requests
            if len(requests_params):
                # calculate how many concurrent requests to fetch record by id we can make, based on the number of active threads and the max_concurrent_requests
                threads = threading.active_count()
                self.logger.info(f"Active threads: {threads}")
                max_requests = max(max_requests - threads, 1)
                self.logger.info(f"Max concurrent requests available to fetch event records by id: {max_requests}")
                for i in range(0, len(requests_params), max_requests):
                    req_params = requests_params[i: i + max_requests]

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_requests
                    ) as executor:
                        futures = {
                            executor.submit(self.get_record_from_events, x["record"], x["decorated_request"]): x for x in req_params
                        }
                        # Process each future as it completes
                        for future in concurrent.futures.as_completed(futures):
                            # Yield records
                            if future.result():
                                yield future.result()
        else:
            for record in records:
                if not record.get("updated") and "created" in record:
                    record["updated"] = record["created"]
                _record = self.get_lines(record, decorated_request)
                yield _record

        return iter([])
    
    def mark_last_item(self, generator):
        iterator = iter(generator)
        try:
            # Attempt to get the first item
            previous = next(iterator)
        except StopIteration:
            # If generator is empty, exit the function
            return
        
        # Iterate through remaining items
        for current in iterator:
            yield previous, False
            previous = current
        
        # Yield the last item with `is_last=True`
        yield previous, True

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_contexts = []
            for record_result, is_last in self.mark_last_item(self.get_records(current_context)):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                ### Modified behaviour
                # Sync children concurrently, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    # invoices child stream is a synthetic stream
                    if self.name == "invoices":
                        self._sync_children(child_context)
                    else:
                        child_contexts.append(child_context)
                        if len(child_contexts) == self.max_concurrent_requests or is_last:
                            with concurrent.futures.ThreadPoolExecutor(
                                max_workers=self.max_concurrent_requests
                            ) as executor:
                                futures = {
                                    executor.submit(self._sync_children, x): x for x in child_contexts
                                }
                            child_contexts = []
                
                ###-

                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()
    
 
class StripeStreamV2(ConcurrentStream):
    """Class for the streams that need to get data from invoice items in full sync and more than one event in incremental syncs """

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # activate flag if it's a full sync to fetch prices from invoiceitems
        if not self.stream_state.get("replication_key"):
            # get data from invoice items
            self.from_invoice_items = True
            yield from super().request_records(context)
            # get data from normal enpoint
            self.from_invoice_items = False
            yield from super().request_records(context)
        else:
            for filter in self.event_filters:
                self.event_filter = filter
                yield from super().request_records(context)

    def parse_response(self, response) -> Iterable[dict]:
        for record in super().parse_response(response):
            if self.from_invoice_items:
                if record["id"] in self.fullsync_ids:
                    continue
            yield record