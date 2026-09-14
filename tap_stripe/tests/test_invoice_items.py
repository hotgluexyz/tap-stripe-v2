"""Tests for invoice_items sync paths."""

from unittest.mock import MagicMock, PropertyMock, patch

from tap_stripe.streams import InvoiceItems, InvoiceLineItems, stripeStream


def make_invoice_line_items_stream():
    """Create an InvoiceLineItems stream instance without SDK initialization."""
    stream = object.__new__(InvoiceLineItems)
    invoice_items_stream = MagicMock()
    invoice_items_stream.fetch_from_parent_stream = False
    invoice_items_stream.selected = True
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


def test_invoice_line_items_skips_sync_when_invoice_items_deselected():
    """By-id invoice_items sync should not run when the stream is deselected."""
    stream = make_invoice_line_items_stream()
    invoice_items_stream = stream._tap.streams["invoice_items"]
    invoice_items_stream.selected = False
    child_context = {"invoice_item_id": "ii_test_001"}

    stream._sync_children(child_context)

    invoice_items_stream.sync.assert_not_called()


def make_invoice_items_stream():
    """Create an InvoiceItems stream instance without SDK initialization."""
    stream = object.__new__(InvoiceItems)
    stream.fetch_from_parent_stream = False
    stream.fetch_pending_items = False
    stream.ids = set()
    type(stream).stream_state = PropertyMock(return_value={})
    return stream


def test_invoice_items_standalone_runs_pending_pass_after_incremental():
    """Standalone sync should list incrementally, then re-list all pending items."""
    stream = make_invoice_items_stream()
    pending_flags = []

    def fake_request_records(self, context):
        pending_flags.append(stream.fetch_pending_items)
        return iter([])

    with patch.object(stripeStream, "request_records", fake_request_records):
        list(stream.request_records({}))

    assert pending_flags == [False, True]
    assert stream.fetch_pending_items is False


def test_invoice_items_by_id_skips_pending_pass():
    """By-id sync should not run the pending list pass."""
    stream = make_invoice_items_stream()
    stream.fetch_from_parent_stream = True

    with patch.object(stripeStream, "request_records", return_value=iter([])) as mock_req:
        list(stream.request_records({"invoice_item_id": "ii_test_001"}))

    mock_req.assert_called_once()
    assert stream.fetch_pending_items is False


def test_invoice_items_get_url_params_pending_pass():
    """Pending list pass should drop created filter and set pending=true."""
    stream = make_invoice_items_stream()
    stream.fetch_pending_items = True

    with patch.object(
        stripeStream, "get_url_params", return_value={"limit": 100, "created[gte]": 123}
    ):
        params = stream.get_url_params(None, None)

    assert params == {"limit": 100, "pending": "true"}
