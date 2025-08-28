"""Stream type classes for tap-stripe-v2."""

from typing import Any, Optional, Iterable, Dict
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
from tap_stripe.client import stripeStream, StripeStreamV2
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_stripe.base_reports import BaseReportsStream

class Invoices(stripeStream):
    """Define Invoices stream."""

    name = "invoices"
    replication_key = "updated"
    event_filter = "invoice.*"
    object = "invoice"

    @property
    def path(self):
        return "events" if self.get_from_events else "invoices"
    
    @property
    def expand(self):
        # not expanding lines discount coupons 'applies_to' here because 
        # stripe only allows expanding up to 4 levels of a property
        if self.get_from_events:
            return ["discounts", "lines.data.discounts", "discounts.coupon.applies_to"]
        else:
            return ["data.discounts", "data.lines.data.discounts", "data.discounts.coupon.applies_to"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("account_country", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_tax_ids", th.ArrayType(th.StringType)),
        th.Property("amount_due", th.IntegerType),
        th.Property("amount_paid", th.NumberType),
        th.Property("amount_remaining", th.NumberType),
        th.Property("application", th.StringType),
        th.Property("application_fee_amount", th.IntegerType),
        th.Property("attempt_count", th.IntegerType),
        th.Property("attempted", th.BooleanType),
        th.Property("auto_advance", th.BooleanType),
        th.Property("automatic_tax", th.CustomType({"type": ["object", "string"]})),
        th.Property("billing_reason", th.StringType),
        th.Property("charge", th.StringType),
        th.Property("collection_method", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("customer", th.StringType),
        th.Property("customer_address", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer_email", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_phone", th.StringType),
        th.Property("customer_shipping", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer_tax_exempt", th.StringType),
        th.Property(
            "customer_tax_ids", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("default_payment_method", th.StringType),
        th.Property("default_source", th.StringType),
        th.Property("default_tax_rates", th.CustomType({"type": ["array", "string"]})),
        th.Property("description", th.StringType),
        th.Property("discount", th.CustomType({"type": ["object", "string"]})),
        th.Property("discounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("due_date", th.DateTimeType),
        th.Property("ending_balance", th.NumberType),
        th.Property("footer", th.StringType),
        th.Property("hosted_invoice_url", th.StringType),
        th.Property("invoice_pdf", th.StringType),
        th.Property("last_finalization_error", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("next_payment_attempt", th.DateTimeType),
        th.Property("number", th.StringType),
        th.Property("on_behalf_of", th.StringType),
        th.Property("paid", th.BooleanType),
        th.Property("paid_out_of_band", th.BooleanType),
        th.Property("payment_intent", th.StringType),
        th.Property("period_end", th.DateTimeType),
        th.Property("period_start", th.DateTimeType),
        th.Property("post_payment_credit_notes_amount", th.IntegerType),
        th.Property("pre_payment_credit_notes_amount", th.IntegerType),
        th.Property("quote", th.StringType),
        th.Property("receipt_number", th.StringType),
        th.Property("redaction", th.StringType),
        th.Property("rendering_options", th.CustomType({"type": ["object", "string"]})),
        th.Property("starting_balance", th.IntegerType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("subtotal", th.IntegerType),
        th.Property("subtotal_excluding_tax", th.IntegerType),
        th.Property("tax", th.NumberType),
        th.Property("test_clock", th.StringType),
        th.Property("total", th.IntegerType),
        th.Property("total_excluding_tax", th.IntegerType),
        th.Property("transfer_data", th.StringType),
        th.Property("webhooks_delivered_at", th.DateTimeType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property(
            "total_discount_amounts", th.CustomType({"type": ["array", "string"]})
        ),
        th.Property("total_tax_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property(
            "payment_settings", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property(
            "status_transitions", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("lines", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        if record:
            invoice_id = record["lines"]["url"].split('/')[3]
            record_lines = []
            for line in record["lines"]["data"]:
                line["invoice_id"] = invoice_id
                record_lines.append(line)
            data = {
                "lines": record_lines
            }
            stripeStream.invoice_lines = data
            return {}

class InvoiceLineItems(stripeStream):
    name = "invoice_line_items"
    parent_stream_type = Invoices
    path = "invoices/{invoice_id}/lines"
    records_jsonpath = "$.[*]"
    get_from_events = False

    @property
    def expand(self):
        return ["data.discounts", "data.discounts.coupon.applies_to"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.IntegerType),
        th.Property("amount_excluding_tax", th.IntegerType),
        th.Property("currency", th.StringType),
        th.Property("subscription_item", th.StringType),
        th.Property("description", th.StringType),
        th.Property("discount_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("discountable", th.BooleanType),
        th.Property(
            "discounts", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("invoice_item", th.StringType),
        th.Property("invoice_id",  th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("period", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.CustomType({"type": ["object", "string"]})),
        th.Property("proration", th.BooleanType),
        th.Property("proration_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("quantity", th.IntegerType),
        th.Property("subscription", th.StringType),
        th.Property(
            "tax_amounts", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property(
            "tax_rates", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("type", th.StringType),
        th.Property("unit_amount_excluding_tax", th.StringType),
    ).to_dict()

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        content = stripeStream.invoice_lines
        if content.get("lines"):
            response = content.get("lines")
            return response

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from extract_jsonpath(self.records_jsonpath, input=response)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        return None
    
    def get_child_context(self, record, context) -> dict:
        if record.get("invoice_item"):
            return {"invoice_item_id": record["invoice_item"]}

    def _sync_children(self, child_context: dict) -> None:
        if child_context is not None:
            return super()._sync_children(child_context)

class InvoiceItems(stripeStream):
    """Define InvoiceItems stream."""

    name = "invoice_items"
    replication_key = "date"
    object = "plan"
    parent_stream_type = InvoiceLineItems
    fetch_from_parent_stream = False
    ids = set()

    @property
    def path(self):
        path = "invoiceitems"
        if self.fetch_from_parent_stream:
            path = "invoiceitems/{invoice_item_id}"
        return path
    
    @property
    def records_jsonpath(self):
        if not self.fetch_from_parent_stream:
            return "$.data[*]"
        return "$.[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.IntegerType),
        th.Property("currency", th.StringType),
        th.Property("customer", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("description", th.StringType),
        th.Property("discountable", th.BooleanType),
        th.Property("invoice", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("proration", th.BooleanType),
        th.Property("quantity", th.IntegerType),
        th.Property("subscription", th.StringType),
        th.Property("subscription_item", th.StringType),
        th.Property("test_clock", th.StringType),
        th.Property("unit_amount", th.IntegerType),
        th.Property("unit_amount_decimal", th.StringType),
        th.Property(
            "tax_rates", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property(
            "discounts", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("period", th.CustomType({"type": ["object", "string"]})),
        th.Property("price", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()
    
    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # 1. fetch all invoices using rep key
        invoice_item_id = None
        if not self.fetch_from_parent_stream:
            # invoice_items is a child stream but it also fetches data using its own rep key
            # so we need to keep the rep_key_value at the header level
            if "replication_key_value" in self.stream_state:
                self.stream_state['starting_replication_value'] = self.stream_state['replication_key_value']
            #---
            invoice_item_id = context.pop("invoice_item_id")
            yield from super().request_records(context)
            self.fetch_from_parent_stream = True
        # 2. fetch invoices from parent stream
        if self.fetch_from_parent_stream:
            if invoice_item_id:
                context.update({"invoice_item_id": invoice_item_id})
            # get invoiceitem ids
            yield from super().request_records(context)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        if self.fetch_from_parent_stream:
            # this params are not allowed for fetching invoiceitems by id
            params.pop("created[gte]", None)
            params.pop("limit", None)
        return params
    
    def post_process(self, row, context) -> dict:
        if row["id"] not in self.ids:
            self.ids.add(row["id"])
            return super().post_process(row, context)

class Subscriptions(stripeStream):
    """Define Subscriptions stream."""

    name = "subscriptions"
    replication_key = "updated"
    event_filter = "customer.*"
    object = "subscription"

    @property
    def path(self):
        return "events" if self.get_from_events else "subscriptions"

    @property
    def expand(self):
        if self.get_from_events:
            return ["discounts"]
        else:
            return ["data.discounts"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("application", th.StringType),
        th.Property("application_fee_percent", th.NumberType),
        th.Property("automatic_tax", th.CustomType({"type": ["object", "string"]})),
        th.Property("billing_cycle_anchor", th.DateTimeType),
        th.Property("billing_thresholds", th.CustomType({"type": ["object", "string"]})),
        th.Property("cancel_at", th.DateTimeType),
        th.Property("cancel_at_period_end", th.BooleanType),
        th.Property("canceled_at", th.DateTimeType),
        th.Property("collection_method", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("current_period_end", th.DateTimeType),
        th.Property("current_period_start", th.DateTimeType),
        th.Property("customer", th.StringType),
        th.Property("days_until_due", th.IntegerType),
        th.Property("default_payment_method", th.StringType),
        th.Property("default_source", th.StringType),
        th.Property(
            "default_tax_rates", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("description", th.StringType),
        th.Property("discount", th.CustomType({"type": ["object", "string"]})),
        th.Property("ended_at", th.DateTimeType),
        th.Property("items", th.CustomType({"type": ["object", "string"]})),
        th.Property("latest_invoice", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("next_pending_invoice_item_invoice", th.StringType),
        th.Property("pause_collection", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_settings", th.CustomType({"type": ["object", "string"]})),
        th.Property("pending_invoice_item_interval", th.StringType),
        th.Property("pending_setup_intent", th.StringType),
        th.Property("pending_update", th.CustomType({"type": ["object", "string"]})),
        th.Property("plan", th.CustomType({"type": ["object", "string"]})),
        th.Property("quantity", th.NumberType),
        th.Property("schedule", th.StringType),
        th.Property("start_date", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("test_clock", th.StringType),
        th.Property("transfer_data", th.StringType),
        th.Property("trial_end", th.DateTimeType),
        th.Property("trial_start", th.DateTimeType),
    ).to_dict()

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        if not self.get_from_events:
            params["status"] = "all"
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"subscription_id": record["id"]}


class SubscriptionItemStream(stripeStream):
    name = "subscription_items"
    path = "subscription_items"
    parent_stream_type = Subscriptions
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("price", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("object", th.StringType),
            th.Property("active", th.BooleanType),
            th.Property("billing_scheme", th.StringType),
            th.Property("created", th.NumberType),
            th.Property("currency", th.StringType),
            th.Property("livemode", th.BooleanType),
            th.Property("lookup_key", th.StringType),
            th.Property("nickname", th.StringType),
            th.Property("product", th.StringType),
            th.Property("recurring", th.ObjectType(
                th.Property("aggregate_usage", th.StringType),
                th.Property("interval", th.StringType),
                th.Property("interval_count", th.IntegerType),
                th.Property("usage_type", th.StringType)
            )),
            th.Property("tax_behavior", th.StringType),
            th.Property("tiers_mode", th.StringType),
            th.Property("type", th.StringType),
            th.Property("unit_amount", th.IntegerType),
            th.Property("unit_amount_decimal", th.StringType)
        )),
        th.Property("quantity", th.IntegerType),
        th.Property("subscription", th.StringType),
        th.Property("tax_rates", th.ArrayType(th.CustomType({"type": ["object", "string"]}))),
    ).to_dict()

    def get_url_params(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        params["subscription"] = context["subscription_id"]
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"subscription_item_id": record["id"]}



class Plans(StripeStreamV2):
    """Define Plans stream."""

    name = "plans"
    replication_key = "updated"
    object = "plan"
    from_invoice_items = False

    @property
    def expand(self):
        if self.get_from_events:
            return ["tiers"]
        else:
            if self.from_invoice_items:
                return ["data.price.tiers"]
            else:
                return ["data.tiers"]

    @property
    def path(self):
        # get prices from invoiceitems and prices in a full sync
        if not self.get_from_events:
            if self.from_invoice_items:
                return "invoiceitems"
            else:
                return "prices" 
        else:   
            return "events"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("aggregate_usage", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("amount_decimal", th.StringType),
        th.Property("billing_scheme", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("interval", th.StringType),
        th.Property("interval_count", th.IntegerType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("nickname", th.StringType),
        th.Property("product", th.StringType),
        th.Property("tiers_mode", th.StringType),
        th.Property("transform_usage", th.CustomType({"type": ["object", "string"]})),
        th.Property("trial_period_days", th.NumberType),
        th.Property(
            "tiers",
            th.ArrayType(
                th.CustomType(
                    {
                        "anyOf": [
                            {
                                "type":["string","null"]
                            },
                            {
                                "type":["object","null"],
                                "properties": {
                                    "flat_amount": {
                                        "type": [
                                        "null",
                                        "integer"
                                        ]
                                    },
                                    "unit_amount": {
                                        "type": [
                                        "null",
                                        "integer"
                                        ]
                                    },
                                    "up_to": {
                                        "type": [
                                        "null",
                                        "integer"
                                        ]
                                    }
                                }
                            }

                        ]
                    }
                )
                    )
        ),
        th.Property("usage_type", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row = super().post_process(row, context)
        row.update({"amount": row.get("unit_amount")})
        row.update({"amount_decimal": row.get("unit_amount_decimal")})
        row.update({"transform_usage": row.get("transform_quantity")})
        row.update({"updated": row.get("created")})

        # process fields for recurring prices
        recurring = row.get("recurring")
        if recurring:
            row.update({"aggregate_usage": recurring.get("aggregate_usage")})
            row.update({"interval": recurring.get("interval")})
            row.update({"interval_count": recurring.get("interval_count")})
            row.update({"trial_period_days": recurring.get("trial_period_days")})
            row.update({"usage_type": recurring.get("usage_type")})
        return row
    
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if self.from_invoice_items:
            items = response.json()["data"]
            prices = [item["price"] for item in items]
            for record in prices:
                # clean dupplicate prices from invoice items
                if record["id"] not in self.fullsync_ids:
                    self.fullsync_ids.append(record["id"])
                    yield record
        else:
            for record in super().parse_response(response):
                if record["id"] not in self.fullsync_ids:
                    self.fullsync_ids.append(record["id"])
                    yield record



class CreditNotes(stripeStream):
    """Define CreditNotes stream."""

    name = "credit_notes"
    replication_key = "updated"
    event_filter = "credit_note.*"
    object = "credit_note"

    @property
    def path(self):
        return "events" if self.get_from_events else "credit_notes"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customer", th.StringType),
        th.Property("customer_balance_transaction", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("invoice", th.StringType),
        th.Property("lines", th.CustomType({"type": ["object", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("memo", th.StringType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("number", th.StringType),
        th.Property("out_of_band_amount", th.NumberType),
        th.Property("pdf", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("refund", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subtotal", th.NumberType),
        th.Property("subtotal_excluding_tax", th.NumberType),
        th.Property("tax_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("total", th.NumberType),
        th.Property("total_excluding_tax", th.NumberType),
        th.Property("type", th.StringType),
        th.Property("voided_at", th.DateTimeType),
    ).to_dict()
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "credit_note_id": record["id"],
        }


class Coupons(stripeStream):
    """Define Coupons stream."""

    name = "coupons"
    replication_key = "updated"
    event_filter = "coupon.*"
    object = "coupon"

    @property
    def path(self):
        return "events" if self.get_from_events else "coupons"
    
    @property
    def expand(self):
        if self.get_from_events:
            return ["applies_to"]
        else:
            return ["data.applies_to"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount_off", th.NumberType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("duration", th.StringType),
        th.Property("duration_in_months", th.IntegerType),
        th.Property("livemode", th.BooleanType),
        th.Property("max_redemptions", th.IntegerType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.StringType),
        th.Property("percent_off", th.NumberType),
        th.Property("redeem_by", th.DateTimeType),
        th.Property("times_redeemed", th.IntegerType),
        th.Property("valid", th.BooleanType),
        th.Property("applies_to", th.CustomType({"type": ["object", "string"]})),
    ).to_dict()


class Products(StripeStreamV2):
    """Define Products stream."""

    name = "products"
    replication_key = "updated"
    object = "product"
    from_invoice_items = False
    
    @property
    def path(self):
        # get products from invoiceitems and products in a full sync
        if not self.get_from_events:
            if self.from_invoice_items:
                return "invoiceitems"
            else:
                return "products" 
        else:   
            return "events"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("default_price", th.StringType),
        th.Property("description", th.StringType),
        th.Property("images", th.CustomType({"type": ["array", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.StringType),
        th.Property("package_dimensions", th.StringType),
        th.Property("shippable", th.BooleanType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("tax_code", th.StringType),
        th.Property("unit_label", th.StringType),
        th.Property("updated", th.DateTimeType),
        th.Property("url", th.StringType),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        if self.from_invoice_items:
            self.get_data_from_id = True
            products = [item["price"] for item in response.json()["data"]]
            [item.update({"id": item["product"]}) for item in products]
            for record in super().parse_response(products):
                yield record
        else:
            for record in super().parse_response(response):
                yield record


class Customers(stripeStream):
    """Define Customers stream."""

    name = "customers"
    replication_key = "updated"
    event_filter = "customer.*"
    object = "customer"

    @property
    def path(self):
        return "events" if self.get_from_events else "customers"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("address", th.CustomType({"type": ["object", "string"]})),
        th.Property("balance", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("default_source", th.StringType),
        th.Property("delinquent", th.BooleanType),
        th.Property("description", th.StringType),
        th.Property("discount", th.CustomType({"type": ["object", "string"]})),
        th.Property("email", th.StringType),
        th.Property("invoice_prefix", th.StringType),
        th.Property("invoice_settings", th.CustomType({"type": ["object", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.StringType),
        th.Property("next_invoice_sequence", th.IntegerType),
        th.Property("phone", th.StringType),
        th.Property("preferred_locales", th.CustomType({"type": ["array", "string"]})),
        th.Property("shipping", th.CustomType({"type": ["object", "string"]})),
        th.Property("tax_exempt", th.StringType),
        th.Property("test_clock", th.StringType),
        th.Property("deleted", th.BooleanType),
    ).to_dict()

    def post_process(self, row, context) -> dict:
        row["deleted"] = row.get("deleted", False)
        return super().post_process(row, context)


class Events(stripeStream):
    """Define Coupons stream."""

    name = "events"
    path = "events"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("api_version", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("data", th.CustomType({"type": ["array", "object", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("pending_webhooks", th.IntegerType),
        th.Property("request", th.CustomType({"type": ["object", "string"]})),
        th.Property("type", th.StringType),
    ).to_dict()


class SubscriptionSchedulesStream(stripeStream):
    """Define stream."""

    name = "subscription_schedules"
    path = "subscription_schedules"
    replication_key = "created"
    object = "subscription_schedule"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("canceled_at", th.DateTimeType),
        th.Property("completed_at", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("current_phase", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer", th.StringType),
        th.Property("default_settings", th.CustomType({"type": ["object", "string"]})),
        th.Property("end_behavior", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("phases", th.CustomType({"type": ["array", "string"]})),
        th.Property("released_at", th.DateTimeType),
        th.Property("released_subscription", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subscription", th.StringType),
    ).to_dict()


class UsageRecordsStream(stripeStream):

    name = "usage_records"
    path = "subscription_items/{subscription_item_id}/usage_record_summaries"
    parent_stream_type = SubscriptionItemStream
    object = "usage_record_summary"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("invoice", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("period", th.CustomType({"type": ["object", "string"]})),
        th.Property("subscription_item", th.StringType),
        th.Property("total_usage", th.IntegerType),
    ).to_dict()

    def validate_response(self, response: requests.Response) -> None:

        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

class TaxRatesStream(stripeStream):

    name = "tax_rates"
    path = "tax_rates"
    object = "tax_rate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("active", th.BooleanType),
        th.Property("country", th.StringType),
        th.Property("description", th.StringType),
        th.Property("display_name", th.StringType),
        th.Property("inclusive", th.BooleanType),
        th.Property("jurisdiction", th.StringType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("percentage", th.NumberType),
        th.Property("state", th.StringType),
        th.Property("state", th.StringType),
        th.Property("tax_type", th.StringType),
        
    ).to_dict()        


class BalanceTransactionsStream(stripeStream):

    name = "balance_transactions"
    path = "balance_transactions"
    object = "balance_transactions"

    @property
    def replication_key(self):
        if self.config.get("incremental_balance_transactions"):
            return "created"
        return None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("available_on", th.NumberType),
        th.Property("created", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("description", th.StringType),
        th.Property("fee", th.NumberType),
        th.Property("fee_details", th.CustomType({"type": ["array", "string"]})),
        th.Property("net", th.NumberType),
        th.Property("reporting_category", th.StringType),
        th.Property("source", th.StringType),
        th.Property("status", th.StringType),
        th.Property("type", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("period", th.CustomType({"type": ["object", "string"]})),
        th.Property("subscription_item", th.StringType),
        th.Property("total_usage", th.IntegerType),
        th.Property("exchange_rate", th.NumberType),
    ).to_dict()

    def apply_catalog(self, catalog) -> None:
        self._tap_input_catalog = catalog
        catalog_entry = catalog.get_stream(self.name)
        if catalog_entry:
            self.primary_keys = catalog_entry.key_properties
            if catalog_entry.replication_method:
                self.forced_replication_method = catalog_entry.replication_method


class ChargesStream(stripeStream):

    name = "charges"
    object = "charge"
    replication_key = "updated"
    event_filter = "charge.*"

    @property
    def path(self):
        return "events" if self.get_from_events else "charges"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("amount_captured", th.NumberType),
        th.Property("amount_refunded", th.NumberType),
        th.Property("application", th.StringType),
        th.Property("application_fee", th.StringType),
        th.Property("application_fee_amount", th.NumberType),
        th.Property("balance_transaction", th.StringType),
        th.Property("billing_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("captured", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customer", th.StringType),
        th.Property("description", th.StringType),
        th.Property("disputed", th.BooleanType),
        th.Property("failure_balance_transaction", th.StringType),
        th.Property("failure_code", th.StringType),
        th.Property("failure_message", th.StringType),
        th.Property("fraud_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("invoice", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("on_behalf_of", th.StringType),
        th.Property("on_behalf_of", th.StringType),
        th.Property("outcome", th.CustomType({"type": ["object", "string"]})),
        th.Property("paid", th.BooleanType),
        th.Property("payment_intent", th.StringType),
        th.Property("payment_method", th.StringType),
        th.Property("payment_method_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("receipt_email", th.StringType),
        th.Property("receipt_url", th.StringType),
        th.Property("refunded", th.BooleanType),
        th.Property("review", th.StringType),
        th.Property("source_transfer", th.StringType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("statement_descriptor_suffix", th.StringType),
        th.Property("status", th.StringType),
        th.Property("transfer_data", th.StringType),
        th.Property("transfer_data", th.StringType),
        th.Property("transfer_group", th.StringType),
    ).to_dict()

class CheckoutSessionsStream(stripeStream):

    name = "checkout_sessions"
    object = "checkout.session"
    replication_key = "updated"

    @property
    def path(self):
        return "events" if self.get_from_events else "checkout/sessions"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("after_expiration", th.CustomType({"type": ["object", "string"]})),
        th.Property("allow_promotion_codes", th.BooleanType),
        th.Property("amount_subtotal", th.NumberType),
        th.Property("amount_total", th.NumberType),
        th.Property("automatic_tax", th.CustomType({"type": ["object", "string"]})),
        th.Property("billing_address_collection", th.CustomType({"type": ["object", "string"]})),
        th.Property("cancel_url", th.StringType),
        th.Property("client_reference_id", th.StringType),
        th.Property("consent", th.CustomType({"type": ["object", "string"]})),
        th.Property("consent_collection", th.CustomType({"type": ["object", "string"]})),
        th.Property("currency", th.StringType),
        th.Property("custom_fields", th.CustomType({"type": ["array", "string"]})),
        th.Property("custom_text", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer_creation", th.StringType),
        th.Property("customer_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer_email", th.StringType),
        th.Property("expires_at", th.NumberType),
        th.Property("invoice", th.StringType),
        th.Property("invoice_creation", th.CustomType({"type": ["object", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("locale", th.StringType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("mode", th.StringType),
        th.Property("payment_intent", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_link", th.StringType),
        th.Property("payment_method_collection", th.StringType),
        th.Property("payment_method_options", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_method_types", th.CustomType({"type": ["array", "string"]})),
        th.Property("payment_status", th.StringType),
        th.Property("phone_number_collection", th.CustomType({"type": ["object", "string"]})),
        th.Property("recovered_from", th.StringType),
        th.Property("setup_intent", th.StringType),
        th.Property("shipping_cost", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipping_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("shipping_options", th.CustomType({"type": ["array", "string"]})),
        th.Property("status", th.StringType),
        th.Property("submit_type", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("success_url", th.StringType),
        th.Property("total_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("url", th.StringType),
       
    ).to_dict()


class CreditNoteLineItemsStream(stripeStream):

    name = "credit_note_line_items"
    path = "credit_notes/{credit_note_id}/lines"
    object = "credit_note_line_item"
    parent_stream_type = CreditNotes
    get_from_events = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("amount_excluding_tax", th.NumberType),
        th.Property("description", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("invoice_line_item", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("quantity", th.NumberType),
        th.Property("tax_amounts", th.CustomType({"type": ["array", "string"]})),
        th.Property("tax_rates", th.CustomType({"type": ["array", "string"]})),
        th.Property("type", th.StringType),
        th.Property("unit_amount", th.NumberType),
        th.Property("unit_amount_decimal", th.StringType),
        th.Property("unit_amount_excluding_tax", th.StringType),
    ).to_dict()
class DisputesIssuingStream(stripeStream):

    name = "disputes_issuing"
    object = "issuing.dispute"
    replication_key = "updated"
    
    @property
    def path(self):
        return "events" if self.get_from_events else "issuing/disputes"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("evidence", th.CustomType({"type": ["object", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("status", th.StringType),
        th.Property("transaction", th.StringType),
        
    ).to_dict()
class PaymentIntentsStream(stripeStream):

    name = "payment_intents"
    object = "payment_intent"
    replication_key = "updated"

    @property
    def path(self):
        return "events" if self.get_from_events else "payment_intents"
    

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("amount", th.NumberType),
        th.Property("amount_capturable", th.NumberType),
        th.Property("amount_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("application", th.StringType),
        th.Property("application_fee_amount", th.NumberType),
        th.Property("application", th.StringType),
        th.Property("automatic_payment_methods", th.CustomType({"type": ["object", "string"]})),
        th.Property("canceled_at", th.NumberType),
        th.Property("cancellation_reason", th.StringType),
        th.Property("capture_method", th.StringType),
        th.Property("client_secret", th.StringType),
        th.Property("confirmation_method", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("description", th.StringType),
        th.Property("last_payment_error", th.CustomType({"type": ["object", "string"]})),
        th.Property("latest_charge", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("next_action", th.CustomType({"type": ["object", "string"]})),
        th.Property("on_behalf_of", th.StringType),
        th.Property("payment_method", th.StringType),
        th.Property("payment_method_options", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_method_types", th.CustomType({"type": ["array", "string"]})),
        th.Property("processing", th.CustomType({"type": ["object", "string"]})),
        th.Property("receipt_email", th.StringType),
        th.Property("review", th.StringType),
        th.Property("setup_future_usage", th.StringType),
        th.Property("shipping", th.CustomType({"type": ["object", "string"]})),
        th.Property("source", th.StringType),
        th.Property("statement_descriptor_suffix", th.StringType),
        th.Property("status", th.StringType),
        th.Property("transfer_data", th.CustomType({"type": ["object", "string"]})),
        th.Property("transfer_group", th.StringType),
        
    ).to_dict()
class PayoutsStream(stripeStream):

    name = "payouts"
    object = "payout"
    replication_key = "updated"
    
    @property
    def path(self):
        return "events" if self.get_from_events else "payouts"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("amount", th.NumberType),
        th.Property("arrival_date", th.DateTimeType),
        th.Property("automatic", th.BooleanType),
        th.Property("balance_transaction", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("description", th.StringType),
        th.Property("destination", th.StringType),
        th.Property("failure_balance_transaction", th.StringType),
        th.Property("failure_code", th.StringType),
        th.Property("failure_message", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("method", th.StringType),
        th.Property("original_payout", th.StringType),
        th.Property("reconciliation_status", th.StringType),
        th.Property("reversed_by", th.StringType),
        th.Property("source_type", th.StringType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("status", th.StringType),
        th.Property("type", th.StringType),
        
    ).to_dict()
class PromotionCodesStream(stripeStream):

    name = "promotion_codes"
    object = "promotion_code"
    replication_key = "updated"
    
    @property
    def path(self):
        return "events" if self.get_from_events else "promotion_codes"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("active", th.BooleanType),
        th.Property("code", th.StringType),
        th.Property("coupon", th.CustomType({"type": ["object", "string"]})),
        th.Property("customer", th.StringType),
        th.Property("expires_at", th.DateTimeType),
        th.Property("livemode", th.BooleanType),
        th.Property("customer", th.StringType),
        th.Property("max_redemptions", th.NumberType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("restrictions", th.CustomType({"type": ["object", "string"]})),
        th.Property("times_redeemed", th.NumberType),
    ).to_dict()

class TransfersStream(stripeStream):

    name = "transfers"
    object = "transfer"
    replication_key = "updated"
    
    @property
    def path(self):
        return "events" if self.get_from_events else "transfers"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("amount", th.NumberType),
        th.Property("amount_reversed", th.NumberType),
        th.Property("balance_transaction", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("description", th.StringType),
        th.Property("destination", th.StringType),
        th.Property("destination_payment", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("reversals", th.CustomType({"type": ["object", "string"]})),
        th.Property("reversed", th.BooleanType),
        th.Property("source_transaction", th.StringType),
        th.Property("source_type", th.StringType),
        th.Property("transfer_group", th.StringType),
        
    ).to_dict()
class RefundsStream(stripeStream):

    name = "refunds"
    replication_key = "updated"
    object = "refund"
    
    @property
    def path(self):
        return "events" if self.get_from_events else "refunds"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("balance_transaction", th.StringType),
        th.Property("charge", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("destination_details", th.ObjectType(
            th.Property("card", th.ObjectType(
                th.Property("reference", th.StringType),
                th.Property("reference_status", th.StringType),
                th.Property("reference_type", th.StringType),
                th.Property("type", th.StringType)
            )),
            th.Property("type", th.StringType)
        )),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_intent", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("receipt_number", th.StringType),
        th.Property("source_transfer_reversal", th.StringType),
        th.Property("status", th.StringType),
        th.Property("transfer_reversal", th.StringType)
).to_dict()   
    
class PayoutReportsStream(BaseReportsStream):

    name = "report_payout_reconciliation"
    #although update is mentioned in docs, it is not part of report's response for some reason. Disabling until requested
    # replication_key = "created"
    """
    There are five types of report mentioned here https://docs.stripe.com/reports/report-types/payout-reconciliation
    For now we shortlisted payout_reconciliation.itemized.5 as our report type. This could be potentially configurable using the `report_type` property in config.py
    """
    report_type = "payout_reconciliation.itemized.5"

    schema = th.PropertiesList(
        th.Property("automatic_payout_id", th.StringType),
        th.Property("automatic_payout_effective_at", th.StringType),
        th.Property("balance_transaction_id", th.StringType),
        th.Property("created_utc", th.DateTimeType),
        th.Property("created", th.DateTimeType),
        th.Property("available_on_utc", th.DateTimeType),
        th.Property("available_on", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("gross", th.StringType),
        th.Property("fee", th.StringType),
        th.Property("net", th.StringType),
        th.Property("reporting_category", th.StringType),
        th.Property("source_id", th.StringType),
        th.Property("description", th.StringType),
        th.Property("customer_facing_amount", th.StringType),
        th.Property("customer_facing_currency", th.StringType),
        th.Property("regulatory_tag", th.StringType),
        th.Property("automatic_payout_effective_at_utc", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_email", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_description", th.StringType),
        th.Property("customer_shipping_address_line1", th.StringType),
        th.Property("customer_shipping_address_line2", th.StringType),
        th.Property("customer_shipping_address_city", th.StringType),
        th.Property("customer_shipping_address_state", th.StringType),
        th.Property("customer_shipping_address_postal_code", th.StringType),
        th.Property("customer_shipping_address_country", th.StringType),
        th.Property("customer_address_line1", th.StringType),
        th.Property("customer_address_line2", th.StringType),
        th.Property("customer_address_city", th.StringType),
        th.Property("customer_address_state", th.StringType),
        th.Property("customer_address_postal_code", th.StringType),
        th.Property("customer_address_country", th.StringType),
        th.Property("shipping_address_line1", th.StringType),
        th.Property("shipping_address_line2", th.StringType),
        th.Property("shipping_address_city", th.StringType),
        th.Property("shipping_address_state", th.StringType),
        th.Property("shipping_address_postal_code", th.StringType),
        th.Property("shipping_address_country", th.StringType),
        th.Property("card_address_line1", th.StringType),
        th.Property("card_address_line2", th.StringType),
        th.Property("card_address_city", th.StringType),
        th.Property("card_address_state", th.StringType),
        th.Property("card_address_postal_code", th.StringType),
        th.Property("card_address_country", th.StringType),
        th.Property("charge_id", th.StringType),
        th.Property("payment_intent_id", th.StringType),
        th.Property("charge_created_utc", th.StringType),
        th.Property("charge_created", th.StringType),
        th.Property("invoice_id", th.StringType),
        th.Property("invoice_number", th.StringType),
        th.Property("subscription_id", th.StringType),
        th.Property("order_id", th.StringType),
        th.Property("payment_method_type", th.StringType),
        th.Property("is_link", th.StringType),
        th.Property("card_brand", th.StringType),
        th.Property("card_funding", th.StringType),
        th.Property("card_country", th.StringType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("dispute_reason", th.StringType),
        th.Property("connected_account_id", th.StringType),
        th.Property("connected_account_name", th.StringType),
        th.Property("connected_account_country", th.StringType),
        th.Property("connected_account_direct_charge_id", th.StringType),
        th.Property("destination_payment_id", th.StringType),
        th.Property(
            "payment_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "refund_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property(
            "transfer_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
    ).to_dict()
    
class TaxReportsStream(BaseReportsStream):
    name = "report_tax"
    report_type = "tax.transactions.itemized.2"

    schema = th.PropertiesList(
        th.Property("country_code", th.StringType),
        th.Property(
            "credit_note_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("currency", th.StringType),
        th.Property("customer_tax_id", th.StringType),
        th.Property("destination_resolved_address_country", th.StringType),
        th.Property("destination_resolved_address_state", th.StringType),
        th.Property("filing_currency", th.StringType),
        th.Property("filing_exchange_rate", th.StringType),
        th.Property("filing_non_taxable_amount", th.StringType),
        th.Property("filing_tax_amount", th.StringType),
        th.Property("filing_taxable_amount", th.StringType),
        th.Property("filing_total", th.StringType),
        th.Property("id", th.StringType),
        th.Property("invoice_metadata[key]", th.CustomType({"type": ["object", "string"]})),
        th.Property("jurisdiction_level", th.StringType),
        th.Property("jurisdiction_name", th.StringType),
        th.Property("line_item_id", th.StringType),
        th.Property("non_taxable_amount", th.StringType),
        th.Property("origin_resolved_address_country", th.StringType),
        th.Property("origin_resolved_address_state", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("quantity_decimal", th.StringType),
        th.Property("refund_metadata[key]", th.CustomType({"type": ["object", "string"]})),
        th.Property("state_code", th.StringType),
        th.Property("subtotal", th.StringType),
        th.Property("tax_amount", th.StringType),
        th.Property("tax_code", th.StringType),
        th.Property("tax_date", th.DateTimeType),
        th.Property("tax_name", th.StringType),
        th.Property("tax_rate", th.StringType),
        th.Property(
            "tax_transaction_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
        th.Property("taxability", th.StringType),
        th.Property("taxability_reason", th.StringType),
        th.Property("taxable_amount", th.StringType),
        th.Property("total", th.StringType),
        th.Property("transaction_date", th.DateTimeType),
        th.Property("type", th.StringType),
    ).to_dict()

class BalanceReportsStream(BaseReportsStream):
    name = "report_balance"
    report_type = "balance_change_from_activity.itemized.6"

    schema = th.PropertiesList(
        th.Property("automatic_payout_effective_at", th.DateTimeType),
        th.Property("automatic_payout_id", th.StringType),
        th.Property("available_on_utc", th.DateTimeType),
        th.Property("balance_transaction_id", th.StringType),
        th.Property("card_address_city", th.StringType),
        th.Property("card_address_country", th.StringType),
        th.Property("card_address_line1", th.StringType),
        th.Property("card_address_line2", th.StringType),
        th.Property("card_address_postal_code", th.StringType),
        th.Property("card_address_state", th.StringType),
        th.Property("card_brand", th.StringType),
        th.Property("card_country", th.StringType),
        th.Property("card_funding", th.StringType),
        th.Property("charge_created_utc", th.DateTimeType),
        th.Property("charge_id", th.StringType),
        th.Property("connected_account_country", th.StringType),
        th.Property("connected_account_direct_charge_id", th.StringType),
        th.Property("connected_account_id", th.StringType),
        th.Property("connected_account_name", th.StringType),
        th.Property("created_utc", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customer_address_city", th.StringType),
        th.Property("customer_address_country", th.StringType),
        th.Property("customer_address_line1", th.StringType),
        th.Property("customer_address_line2", th.StringType),
        th.Property("customer_address_postal_code", th.StringType),
        th.Property("customer_address_state", th.StringType),
        th.Property("customer_description", th.StringType),
        th.Property("customer_email", th.StringType),
        th.Property("customer_facing_amount", th.StringType),
        th.Property("customer_facing_currency", th.StringType),
        th.Property("customer_id", th.StringType),
        th.Property("customer_name", th.StringType),
        th.Property("customer_shipping_address_city", th.StringType),
        th.Property("customer_shipping_address_country", th.StringType),
        th.Property("customer_shipping_address_line1", th.StringType),
        th.Property("customer_shipping_address_line2", th.StringType),
        th.Property("customer_shipping_address_postal_code", th.StringType),
        th.Property("customer_shipping_address_state", th.StringType),
        th.Property("description", th.StringType),
        th.Property("dispute_reason", th.StringType),
        th.Property("fee", th.StringType),
        th.Property("gross", th.StringType),
        th.Property("invoice_id", th.StringType),
        th.Property("invoice_number", th.StringType),
        th.Property("is_link", th.StringType),
        th.Property("net", th.StringType),
        th.Property("payment_intent_id", th.StringType),
        th.Property("payment_metadata[key]", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_method_type", th.StringType),
        th.Property("refund_metadata[key]", th.CustomType({"type": ["object", "string"]})),
        th.Property("regulatory_tag", th.StringType),
        th.Property("reporting_category", th.StringType),
        th.Property("shipping_address_city", th.StringType),
        th.Property("shipping_address_country", th.StringType),
        th.Property("shipping_address_line1", th.StringType),
        th.Property("shipping_address_line2", th.StringType),
        th.Property("shipping_address_postal_code", th.StringType),
        th.Property("shipping_address_state", th.StringType),
        th.Property("source_id", th.StringType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("subscription_id", th.StringType),
        th.Property("trace_id", th.StringType),
        th.Property("trace_id_status", th.StringType),
        th.Property(
            "transfer_metadata[key]", th.CustomType({"type": ["object", "string"]})
        ),
    ).to_dict()

class DisputesStream(stripeStream):
    name = "disputes"
    object = "dispute"
    replication_key = "updated"

    @property
    def path(self):
        return "events" if self.get_from_events else "disputes"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("charge", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("evidence", th.CustomType({"type": ["object", "string"]})),
        th.Property("evidence_details", th.CustomType({"type": ["object", "string"]})),
        th.Property("is_charge_refundable", th.BooleanType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("payment_intent", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("status", th.StringType),
    ).to_dict()


class Discounts(stripeStream):
    """Define Products stream."""

    name = "discounts"
    replication_key = "updated"
    object = "invoice"
    event_filter = "invoice.*"
    
    @property
    def path(self):
        # get discounts from invoices and invoice line items
        if not self.get_from_events:
            return "invoices"
        else:   
            return "events"


    @property
    def expand(self):
        if self.get_from_events:
            return ["discounts", "lines.data.discounts", "discounts.coupon.applies_to"]
        else:
            return ["data.discounts", "data.lines.data.discounts", "data.discounts.coupon.applies_to"]
        

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("checkout_session", th.StringType),
        th.Property("coupon", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("object", th.StringType),
            th.Property("amount_off", th.IntegerType),
            th.Property("created", th.IntegerType),
            th.Property("currency", th.StringType),
            th.Property("duration", th.StringType),
            th.Property("duration_in_months", th.IntegerType),
            th.Property("max_redemptions", th.IntegerType),
            th.Property("metadata", th.CustomType({"type": ["object", "array", "string"]})),
            th.Property("name", th.StringType),
            th.Property("percent_off", th.NumberType),
            th.Property("redeem_by", th.IntegerType),
            th.Property("times_redeemed", th.IntegerType),
            th.Property("valid", th.BooleanType),
        )),
        th.Property("customer", th.StringType),
        th.Property("end", th.DateTimeType),
        th.Property("invoice", th.StringType),
        th.Property("invoice_item", th.StringType),
        th.Property("promotion_code", th.StringType),
        th.Property("start", th.DateTimeType),
        th.Property("subscription", th.BooleanType),
        th.Property("subscription_item", th.StringType),
        th.Property("updated", th.DateTimeType),
    ).to_dict()

    def parse_response(self, response) -> Iterable[dict]:
        response = super().parse_response(response)
        discounts = []
        for invoice in response:
            invoice_discounts = []
            updated = invoice["updated"]
            # add header discounts to invoice discounts list
            invoice_discounts.extend(invoice["discounts"])
            # add line discounts to invoice discounts list
            [invoice_discounts.extend(line["discounts"]) for line in invoice["lines"]["data"]]
            # add updated rep key to all invoice discounts
            [discount.update({"updated": updated}) for discount in invoice_discounts]
            # add invoice discounts to discounts list
            discounts.extend(invoice_discounts)
        return discounts
