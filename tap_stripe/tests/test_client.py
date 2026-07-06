"""Tests for Stripe client error classification."""

import json

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError

from tap_stripe.client import stripeStream
from tap_stripe.streams import UsageRecordsStream


class TestStripeStream(stripeStream):
    """Minimal stream for response validation tests."""

    name = "events"
    path = "events"


class TestUsageRecordsValidationStream(UsageRecordsStream):
    """Minimal usage-records stream for response validation tests."""


def make_response(status_code, payload):
    """Build a JSON response object for tests."""
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload).encode("utf-8")
    response.headers["Content-Type"] = "application/json"
    return response


def make_stream():
    """Create a stream instance without initializing the SDK base class."""
    stream = object.__new__(TestStripeStream)
    stream.ignore_statuscode = []
    stream.extra_retry_statuses = []
    return stream


def test_validate_response_classifies_permission_denied_as_invalid_credentials():
    """Treat any 403 response as invalid credentials in the base client path."""
    stream = make_stream()
    response = make_response(
        403,
        {
            "error": {
                "type": "permission_denied",
                "message": "This restricted key does not have the required event_read permission.",
            }
        },
    )

    with pytest.raises(InvalidCredentialsError):
        stream.validate_response(response)


def test_usage_records_validate_response_classifies_403_as_invalid_credentials():
    """Treat any 403 response as invalid credentials in the usage-records path."""
    stream = object.__new__(TestUsageRecordsValidationStream)
    stream.ignore_statuscode = []
    stream.extra_retry_statuses = []
    stream.path = "subscription_items/{subscription_item_id}/usage_record_summaries"
    response = make_response(403, {"error": {"message": "Forbidden"}})

    with pytest.raises(InvalidCredentialsError):
        stream.validate_response(response)
