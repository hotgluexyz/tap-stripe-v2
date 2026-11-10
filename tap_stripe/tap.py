"""stripe tap class."""

import json
import platform
from importlib import metadata
from typing import List

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_stripe.streams import (
    Accounts,
    Coupons,
    CreditNotes,
    Customers,
    CustomersParentStream,
    CustomerBalanceTransactionsStream,
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
    Accounts,
    Coupons,
    CreditNotes,
    Customers,
    CustomersParentStream,
    CustomerBalanceTransactionsStream,
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

class Tapstripe(Tap):
    """stripe tap class."""

    name = "tap-stripe-v2"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = th.PropertiesList(
        th.Property("client_secret", th.StringType, required=True),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2000-01-01T00:00:00.000Z",
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        sdk = metadata.distribution("hotglue-singer-sdk")
        sdk_origin = json.loads(sdk.read_text("direct_url.json"))
        self.logger.info(
            "HGI-11648 validation: Python=%s jsonpath-ng=%s SDK=%s SDK-commit=%s",
            platform.python_version(),
            metadata.version("jsonpath-ng"),
            sdk.version,
            sdk_origin["vcs_info"]["commit_id"],
        )
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

    @property
    def streams(self) -> dict:
        """Mark parent-only streams hidden in catalog metadata."""
        input_catalog = self.input_catalog

        if self._streams is None:
            self._streams = {}
            for stream in self.load_streams():
                if input_catalog is not None:
                    stream.apply_catalog(input_catalog)
                root_metadata = stream.metadata[()]
                visible = getattr(stream, "visible_in_catalog", True)
                root_metadata.visible = visible
                # Catalog-free syncs select every stream. Keep hidden parents
                # unselected so they are not written on their own.
                if input_catalog is None and not visible:
                    root_metadata.selected = False
                self._streams[stream.name] = stream
        return self._streams


if __name__ == "__main__":
    Tapstripe.cli()
