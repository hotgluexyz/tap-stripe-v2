"""Stream type classes for tap-stripe-v2."""

from singer_sdk import typing as th

from tap_stripe.client import stripeStream


class Invoices(stripeStream):
    """Define Invoices stream."""

    name = "invoices"
    path = "invoices"
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
        th.Property("rendering_options", th.StringType),
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


class InvoiceItems(stripeStream):
    """Define InvoiceItems stream."""

    name = "invoice_items"
    path = "invoiceitems"
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
    replication_key = "created"
    params = {"status": "all"}

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


class Plans(stripeStream):
    """Define Plans stream."""

    name = "plans"
    path = "plans"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("aggregate_usage", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("amount_decimal", th.StringType),
        th.Property("billing_scheme", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("interval", th.StringType),
        th.Property("interval_count", th.IntegerType),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("nickname", th.StringType),
        th.Property("product", th.StringType),
        th.Property("tiers_mode", th.StringType),
        th.Property("transform_usage", th.CustomType({"type": ["object", "string"]})),
        th.Property("trial_period_days", th.StringType),
        th.Property("usage_type", th.StringType),
    ).to_dict()


class CreditNotes(stripeStream):
    """Define CreditNotes stream."""

    name = "credit_notes"
    path = "credit_notes"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount", th.NumberType),
        th.Property("created", th.DateTimeType),
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
    path = "coupons"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("amount_off", th.NumberType),
        th.Property("created", th.DateTimeType),
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
    path = "products"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("default_price", th.StringType),
        th.Property("description", th.StringType),
        th.Property("images", th.CustomType({"type": ["array", "string"]})),
        th.Property("livemode", th.BooleanType),
        th.Property("metadata", th.CustomType({"type": ["object", "string"]})),
        th.Property("name", th.StringType),
        th.Property("package_dimensions", th.StringType),
        th.Property("shippable", th.StringType),
        th.Property("statement_descriptor", th.StringType),
        th.Property("tax_code", th.StringType),
        th.Property("unit_label", th.StringType),
        th.Property("updated", th.DateTimeType),
        th.Property("url", th.StringType),
    ).to_dict()


class Customers(stripeStream):
    """Define Customers stream."""

    name = "customers"
    path = "customers"
    replication_key = "created"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("object", th.StringType),
        th.Property("address", th.CustomType({"type": ["object", "string"]})),
        th.Property("balance", th.IntegerType),
        th.Property("created", th.DateTimeType),
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
