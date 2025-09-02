"""Stream type classes for tap-stripe-v2."""

from typing import Any, Optional, Iterable, Dict, cast
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from tap_stripe.client import stripeStream, StripeStreamV2, ConcurrentStream
from urllib.parse import urlencode
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from backports.cached_property import cached_property
import csv
from io import StringIO
import time
from dateutil.parser import parse
from datetime import datetime


class Accounts(stripeStream):
    """Define Accounts stream."""

    name = "accounts"
    replication_key = None
    object = "account"
    path = "accounts"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("business_profile", th.CustomType({"type": ["object", "string"]})),
        th.Property("business_type", th.StringType),
        th.Property("capabilities", th.CustomType({"type": ["object", "string"]})),
        th.Property("charges_enabled", th.BooleanType),
        th.Property("controller", th.CustomType({"type": ["object", "string"]})),
        th.Property("country", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("default_currency", th.StringType),
        th.Property("details_submitted", th.BooleanType),
        th.Property("email", th.StringType),
        th.Property("external_accounts", th.CustomType({"type": ["object", "string"]})),
        th.Property("future_requirements", th.CustomType({"type": ["object", "string"]})),
        th.Property("login_links", th.CustomType({"type": ["object", "string"]})),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("payouts_enabled", th.BooleanType),
        th.Property("requirements", th.CustomType({"type": ["object", "string"]})),
        th.Property("settings", th.CustomType({"type": ["object", "string"]})),
        th.Property("tos_acceptance", th.CustomType({"type": ["object", "string"]})),
        th.Property("type", th.StringType),
        th.Property("updated", th.DateTimeType)
    ).to_dict()

class Invoices(ConcurrentStream):
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
        th.Property("last_finalization_error", th.CustomType({"type": ["object", "string"]})),
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
        th.Property("transfer_data", th.ObjectType(
            th.Property("amount", th.IntegerType),
            th.Property("destination", th.CustomType({"type": ["object", "string"]})),
        )),
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


class Subscriptions(ConcurrentStream):
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
        th.Property("pending_invoice_item_interval", th.ObjectType(
            th.Property("interval", th.StringType),
            th.Property("interval_count", th.IntegerType),
        )),
        th.Property("pending_setup_intent", th.StringType),
        th.Property("pending_update", th.CustomType({"type": ["object", "string"]})),
        th.Property("plan", th.CustomType({"type": ["object", "string"]})),
        th.Property("quantity", th.NumberType),
        th.Property("schedule", th.StringType),
        th.Property("start_date", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("test_clock", th.StringType),
        th.Property("transfer_data", th.ObjectType(
            th.Property("amount", th.IntegerType),
            th.Property("destination", th.CustomType({"type": ["object", "string"]})),
        )),
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
    event_filters = ["price.created", "price.updated", "customer.subscription.updated", "invoice.updated"]
    plan_ids = set()
    

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

        if row["id"] in self.plan_ids:
            return None
        else:
            self.plan_ids.add(row["id"])
        
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
                yield record


class CreditNotes(ConcurrentStream):
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


class Coupons(ConcurrentStream):
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
    event_filters = ["product.updated", "product.created", "invoice.updated"]
    
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


class Customers(ConcurrentStream):
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
    ).to_dict()


class Events(ConcurrentStream):
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


class SubscriptionSchedulesStream(ConcurrentStream):
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


class BalanceTransactionsStream(ConcurrentStream):

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


class ChargesStream(ConcurrentStream):

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
        th.Property("transfer_data", th.ObjectType(
            th.Property("amount", th.IntegerType),
            th.Property("destination", th.StringType),
        )),
        th.Property("transfer_group", th.StringType),
    ).to_dict()


class CheckoutSessionsStream(ConcurrentStream):

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

    
class DisputesIssuingStream(ConcurrentStream):

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


class PaymentIntentsStream(ConcurrentStream):
    name = "payment_intents"
    object = "payment_intent"
    replication_key = "updated"
    event_filter = "payment_intent.*"

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


class PayoutsStream(ConcurrentStream):

    name = "payouts"
    object = "payout"
    replication_key = "updated"
    event_filter = "payout.*"
    
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


class PromotionCodesStream(ConcurrentStream):

    name = "promotion_codes"
    object = "promotion_code"
    replication_key = "updated"
    event_filter = "promotion_code.*"
    
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


class TransfersStream(ConcurrentStream):

    name = "transfers"
    object = "transfer"
    replication_key = "updated"
    event_filter = "transfer.*"
    
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


class RefundsStream(ConcurrentStream):

    name = "refunds"
    replication_key = "updated"
    object = "refund"
    event_filter = "refund.*"
    
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
        th.Property("payment_intent", th.CustomType({"type": ["object", "string"]})),
        th.Property("reason", th.StringType),
        th.Property("receipt_number", th.StringType),
        th.Property("source_transfer_reversal", th.StringType),
        th.Property("status", th.StringType),
        th.Property("transfer_reversal", th.StringType)
    ).to_dict()


class PayoutReportsStream(stripeStream):

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
    
    def get_custom_headers(self):
        headers = self.http_headers
        #get the headers with auth token populated
        auth_headers = self.authenticator.auth_headers
        headers.update(auth_headers)
        return headers
    
    def get_report_ranges(self):
        """We can request stripe for available data ranges for a given report type
        This is safer option because we will always get a valid response for valid ranges.
        Otherwise stripe will raise an error.
        Returns:
            available starting and ending date ranges.
        """
        url = f"{self.url_base}reporting/report_types/{self.report_type}"
        resp = requests.get(url=url,headers=self.get_custom_headers())
        self.validate_response(resp)
        data = resp.json()
        return data['data_available_start'],data['data_available_end']
    
    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties
    
    def create_report(self,start_date,end_date):
        url = f"{self.url_base}reporting/report_runs"
        headers = self.get_custom_headers()
        body = {}
        # The report data and processing time will vary based on report type and requested interval.
        body['report_type'] = self.report_type
        body['parameters[interval_start]'] = start_date
        body['parameters[interval_end]'] = end_date
        body = list(body.items())
        #Not ready for production
        for column in self.selected_properties:
            body.append(("parameters[columns][]", column))
            
        # body['parameters'] = parameters
        #Make the request
        response = requests.post(url=url,headers=headers,data=body)
        self.validate_response(response)
        data = response.json()
        return data
    
    def verify_report(self,report_id):
        res = {}
        #keep checking for report status until report is ready for download
        while True:
            headers = self.get_custom_headers()
            url = f"{self.url_base}reporting/report_runs/{report_id}"
            response = requests.get(url,headers=headers)
            self.validate_response(response)
            data = response.json()
            #Stripe will return processing status in "status" property of the response
            if data['status']=="succeeded" and "result" in data:
                res =  data['result']['url']
                break
            #wait for 30 seconds before checking again
            time.sleep(30)
        return res    

    def read_csv_from_url(self,url):
        try:
            # Send GET request to the URL to fetch the CSV data
            headers = self.get_custom_headers()
            response = requests.get(url,headers=headers)
            self.validate_response(response)
            csv_file = StringIO(response.text)

            # Create a CSV DictReader from the response content
            data = csv.DictReader(csv_file,delimiter=',')
            return data

        except requests.exceptions.RequestException as e:
            raise(f"Error fetching CSV from URL: {e}")
            
        except csv.Error as e:
            raise(f"Error parsing CSV data: {e}")
            
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for field in self.datetime_fields:
            if row.get(field):
                # Payout stream have valid formatted dates instead of unix timestamp
                dt_field = parse(row[field])
                if isinstance(dt_field, datetime):
                    row[field] = dt_field.isoformat()
        return row

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Stripe provides a CSV report that can be used to get all records.
        to get all the records we need to do the following steps:
        1. Create/request a report
        2. Periodically check status of the report if its done.
        3. Download the CSV and process. it
        """
        #@TODO use this only if there is no incremental state present. 
        start_date, end_date = self.get_report_ranges()
        report = self.create_report(start_date,end_date)
        if report.get('result') is not None:
            #This means report was already processed and download url is already available
            report_file = report['result']['url']
        else:
            """
            If a report in given range and type is already requested stripe will return previously created report's detail.
            In this case will start verifying the report.
            """
            report_file = self.verify_report(report['id'])
        records = self.read_csv_from_url(report_file)    
        for record in records:
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record


class DisputesStream(ConcurrentStream):
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


class Discounts(ConcurrentStream):
    """Define Products stream."""

    name = "discounts"
    replication_key = "updated"
    object = "invoice"
    event_filter = "invoice.*"
    discount_ids = set()
    
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
            if not invoice.get("updated"):
                updated = invoice["created"] # most records don't have updated field, so we use created for fullsyncs and event date for incremental syncs
            else:
                updated = invoice["updated"] # if updated field is present, use it -> added in parent post process
            # add header discounts to invoice discounts list
            invoice_discounts.extend(invoice["discounts"])
            # add line discounts to invoice discounts list
            [invoice_discounts.extend(line["discounts"]) for line in invoice["lines"]["data"]]
            # add updated rep key to all invoice discounts
            [discount.update({"updated": updated}) for discount in invoice_discounts]

            # check for duplicates
            for discount in invoice_discounts:
                if discount["id"] not in self.discount_ids:
                    self.discount_ids.add(discount["id"])
                    
                    # add invoice discounts to discounts list
                    discounts.append(discount)

        return discounts


