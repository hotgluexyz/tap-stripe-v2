"""Stream type classes for tap-stripe-v2."""

from singer_sdk import typing as th

from tap_stripe.client import stripeStream


class Invoices(stripeStream):
    """Define Invoices stream."""

    name = "invoices"
    path = "invoices"
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("account_country", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_tax_ids", th.StringType),
        th.Property("amount_due", th.IntegerType),
        th.Property("amount_paid", th.NumberType),
        th.Property("amount_remaining", th.NumberType),
        th.Property("application", th.StringType),
        th.Property("application_fee_amount", th.StringType),
        th.Property("attempt_count", th.IntegerType),
        th.Property("attempted", th.BooleanType),
        th.Property("auto_advance", th.BooleanType),
        th.Property("automatic_tax", th.CustomType({"type": ["object", "string"]})),
        th.Property("billing_reason", th.StringType),
        th.Property("charge", th.StringType),
        th.Property("collection_method", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("custom_fields", th.StringType),
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
        th.Property("discount", th.StringType),
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
        th.Property("rendering_options", th.StringType),
        th.Property("starting_balance", th.IntegerType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("subtotal", th.IntegerType),
        th.Property("subtotal_excluding_tax", th.IntegerType),
        th.Property("tax", th.StringType),
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


class InvoiceItems(stripeStream):
    """Define InvoiceItems stream."""

    name = "invoice_items"
    path = "invoiceitems"
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    replication_key = "date"

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
    path = "subscriptions"
    primary_keys = ["id"]
    records_jsonpath = "$.data[*]"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("application", th.StringType),
        th.Property("application_fee_percent", th.StringType),
        th.Property("automatic_tax", th.CustomType({"type": ["object", "string"]})),
        th.Property("billing_cycle_anchor", th.DateTimeType),
        th.Property("billing_thresholds", th.StringType),
        th.Property("cancel_at", th.DateTimeType),
        th.Property("cancel_at_period_end", th.BooleanType),
        th.Property("canceled_at", th.DateTimeType),
        th.Property("collection_method", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("current_period_end", th.DateTimeType),
        th.Property("current_period_start", th.DateTimeType),
        th.Property("customer", th.StringType),
        th.Property("days_until_due", th.StringType),
        th.Property("default_payment_method", th.StringType),
        th.Property("default_source", th.StringType),
        th.Property(
            "default_tax_rates", th.CustomType({"type": ["array", "object", "string"]})
        ),
        th.Property("description", th.StringType),
        th.Property("discount", th.StringType),
        th.Property("ended_at", th.DateTimeType),
        th.Property("items", th.CustomType({"type": ["object", "string"]})),
        th.Property("latest_invoice", th.StringType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("next_pending_invoice_item_invoice", th.StringType),
        th.Property("pause_collection", th.StringType),
        th.Property("payment_settings", th.CustomType({"type": ["object", "string"]})),
        th.Property("pending_invoice_item_interval", th.StringType),
        th.Property("pending_setup_intent", th.StringType),
        th.Property("pending_update", th.StringType),
        th.Property("schedule", th.StringType),
        th.Property("start_date", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("test_clock", th.StringType),
        th.Property("transfer_data", th.StringType),
        th.Property("trial_end", th.DateTimeType),
        th.Property("trial_start", th.DateTimeType),
    ).to_dict()
