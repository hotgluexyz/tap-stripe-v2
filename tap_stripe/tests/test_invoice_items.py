"""Tests for invoice_items by-id fetch from invoice line items."""

from unittest.mock import MagicMock

from tap_stripe.streams import InvoiceLineItems


def make_invoice_line_items_stream():
    """Create an InvoiceLineItems stream instance without SDK initialization."""
    stream = object.__new__(InvoiceLineItems)
    invoice_items_stream = MagicMock()
    invoice_items_stream.fetch_from_parent_stream = False
    stream._tap = MagicMock()
    stream._tap.streams = {"invoice_items": invoice_items_stream}
    return stream


def test_invoice_line_items_syncs_invoice_item_by_id_from_parent():
    """Line items with invoice_item should trigger a by-id invoice_items sync."""
    stream = make_invoice_line_items_stream()
    invoice_items_stream = stream._tap.streams["invoice_items"]
    child_context = {"invoice_item_id": "ii_test_001"}

    stream._sync_children(child_context)

    assert invoice_items_stream.fetch_from_parent_stream is False
    invoice_items_stream.sync.assert_called_once_with(context=child_context)


def test_invoice_line_items_skips_sync_without_invoice_item_id():
    """Line items without invoice_item should not trigger invoice_items sync."""
    stream = make_invoice_line_items_stream()
    invoice_items_stream = stream._tap.streams["invoice_items"]

    stream._sync_children({})
    stream._sync_children(None)

    invoice_items_stream.sync.assert_not_called()
