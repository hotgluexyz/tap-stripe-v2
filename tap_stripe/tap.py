"""stripe tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_stripe.streams import (
    Coupons,
    CreditNotes,
    Customers,
    Events,
    InvoiceItems,
    InvoiceLineItems,
    Invoices,
    Plans,
    Products,
    Subscriptions,
    SubscriptionItemStream,
    SubscriptionSchedulesStream,
    UsageRecordsStream,
    BalanceTransactionsStream,
    ChargesStream,
    CheckoutSessionsStream,
    CreditNoteLineItemsStream,
    DisputesStream,
    PaymentIntentsStream,
    PayoutsStream,
    PromotionCodesStream,
    TransfersStream,
    TaxRatesStream,
    RefundsStream,
    DisputesIssuingStream,
    Discounts,
    PayoutReportsStream,
    TaxReportsStream,
    BalanceReportsStream,
)

STREAM_TYPES = [
    Coupons,
    CreditNotes,
    Customers,
    Events,
    InvoiceItems,
    InvoiceLineItems,
    Invoices,
    Plans,
    Products,
    Subscriptions,
    SubscriptionItemStream,
    SubscriptionSchedulesStream,
    UsageRecordsStream,
    BalanceTransactionsStream,
    ChargesStream,
    CheckoutSessionsStream,
    CreditNoteLineItemsStream,
    DisputesStream,
    PaymentIntentsStream,
    PayoutsStream,
    PromotionCodesStream,
    TransfersStream,
    TaxRatesStream,
    RefundsStream,
    DisputesIssuingStream,
    Discounts,
    PayoutReportsStream,
    TaxReportsStream,
    BalanceReportsStream,
]

default_reports = [{"report": "payouts"}]


class Tapstripe(Tap):
    """stripe tap class."""

    name = "tap-stripe-v2"

    config_jsonschema = th.PropertiesList(
        th.Property("client_secret", th.StringType, required=True),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00.000Z",
            description="The earliest record date to sync",
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # Add standard streams
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    Tapstripe.cli()
