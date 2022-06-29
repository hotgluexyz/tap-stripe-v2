"""stripe tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_stripe.streams import InvoiceItems, Invoices, Subscriptions

STREAM_TYPES = [Invoices, InvoiceItems, Subscriptions]


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
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapstripe.cli()
