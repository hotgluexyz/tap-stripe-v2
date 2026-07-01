"""Tests for Stripe client error classification."""

import json

import pytest
import requests
from hotglue_etl_exceptions import InvalidCredentialsError
from hotglue_singer_sdk.exceptions import FatalAPIError

from tap_stripe.client import stripeStream


class TestStripeStream(stripeStream):
    """Minimal stream for response validation tests."""

    name = "events"
    path = "events"


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
    """Treat restricted-key permission errors as invalid credentials."""
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


def test_validate_response_keeps_other_forbidden_errors_fatal():
    """Keep unrelated forbidden responses classified as fatal errors."""
    stream = make_stream()
    response = make_response(
        403,
        {"error": {"type": "invalid_request_error", "message": "Request failed."}},
    )

    with pytest.raises(FatalAPIError):
        stream.validate_response(response)
