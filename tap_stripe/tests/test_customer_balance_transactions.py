"""Tests for the hidden customers parent of balance transactions."""

from tap_stripe.streams import (
    CustomerBalanceTransactionsStream,
    Customers,
    CustomersParentStream,
)
from tap_stripe.tap import Tapstripe


def _root_metadata(catalog, stream_id):
    stream = next(item for item in catalog["streams"] if item["tap_stream_id"] == stream_id)
    return next(item["metadata"] for item in stream["metadata"] if item["breadcrumb"] == [])


def test_balance_transactions_use_hidden_customer_parent():
    """Balance transactions list every customer without changing customers sync."""
    assert CustomerBalanceTransactionsStream.parent_stream_type is CustomersParentStream
    assert CustomerBalanceTransactionsStream.ignore_parent_replication_key is False
    assert CustomersParentStream.visible_in_catalog is False
    assert CustomersParentStream.path == "customers"


def test_hidden_parent_is_not_visible_in_the_catalog():
    """Discover marks the parent stream hidden and leaves customers visible."""
    tap = Tapstripe(
        config={"client_secret": "sk_test", "start_date": "2020-01-01T00:00:00Z"},
        validate_config=False,
    )
    catalog = tap.catalog_dict
    assert _root_metadata(catalog, "customers_parent")["visible"] is False
    assert _root_metadata(catalog, "customers")["visible"] is True
    assert CustomerBalanceTransactionsStream.parent_stream_type is not Customers
