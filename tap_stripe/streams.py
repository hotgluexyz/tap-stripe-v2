"""Stream type classes for tap-stripe-v2."""

from typing import Any, Optional, Iterable
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from tap_stripe.client import stripeStream  
from urllib.parse import urlencode
import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath


class Invoices(stripeStream):
    """Define Invoices stream."""

    name = "invoices"
    replication_key = "updated"
    event_filter = "invoice.*"
    object = "invoice"

    @property
    def path(self):
        return "events" if self.get_from_events else "invoices"

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


class InvoiceItems(stripeStream):
    """Define InvoiceItems stream."""

    name = "invoice_items"
    path = "invoiceitems"
    replication_key = "date"
    object = "plan"

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


class Subscriptions(stripeStream):
    """Define Subscriptions stream."""

    name = "subscriptions"
    replication_key = "updated"
    event_filter = "customer.*"
    object = "subscription"

    @property
    def path(self):
        return "events" if self.get_from_events else "subscriptions"

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
        th.Property("pending_update", th.StringType),
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
    
    

class Plans(stripeStream):
    """Define Plans stream."""

    name = "plans"
    replication_key = "updated"
    event_filter = "plan.*"
    object = "plan"

    def expand(self,second_request = False):
        if not self.get_from_events: 
            return "data.tiers"
        elif second_request:
            return "tiers"

    @property
    def path(self):
        return "events" if self.get_from_events else "plans"

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
        return row


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


class Coupons(stripeStream):
    """Define Coupons stream."""

    name = "coupons"
    replication_key = "updated"
    event_filter = "coupon.*"
    object = "coupon"

    @property
    def path(self):
        return "events" if self.get_from_events else "coupons"

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
    ).to_dict()


class Products(stripeStream):
    """Define Products stream."""

    name = "products"
    replication_key = "updated"
    event_filter = "product.*"
    object = "product"

    @property
    def path(self):
        return "events" if self.get_from_events else "products"

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
    ).to_dict()


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
      

    
