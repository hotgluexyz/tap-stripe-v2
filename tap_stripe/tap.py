"""stripe tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_stripe.custom_reports import (
    REPORT_CONFIGS,
    CustomReportStream,
    create_report_config,
)
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
        ),
        th.Property(
            "custom_reports",
            th.ArrayType(
                th.ObjectType(th.Property("report", th.StringType, required=True))
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]
        streams = []

        # Add custom report streams if configured
        custom_reports = self.config.get("custom_reports", default_reports)
        for custom_report in custom_reports:
            report_type = custom_report.get("report")
            if report_type not in REPORT_CONFIGS:
                raise ValueError(
                    f"Invalid report type: {report_type}. Must be one of: {', '.join(REPORT_CONFIGS.keys())}"
                )
            if report_type:
                # Create the report config
                report_config = create_report_config(report_type)

                # Create the custom stream
                custom_stream = CustomReportStream(self, report_config)
                custom_stream.replication_key = REPORT_CONFIGS[report_type].get(
                    "replication_key"
                )
                streams.append(custom_stream)

                self.logger.info(
                    f"Added custom report stream: {custom_stream.name} (report={report_type})"
                )
            else:
                self.logger.warning(
                    f"Invalid custom report config: {custom_report}. "
                    "Must have 'report' fields."
                )
        return streams


if __name__ == "__main__":
    Tapstripe.cli()
