import unittest
from unittest.mock import patch
import requests
from tap_frontapp.http import Client, RateLimitException, MetricsRateLimitException
from requests.exceptions import Timeout, ConnectionError
import json


class MockResponse:
    """Mock response object to simulate API calls."""
    def __init__(self, status_code, headers=None, json_data=None, raise_for_status=False):
        self.status_code = status_code
        self.headers = headers or {}
        self._json_data = json_data or {}
        self._raise_for_status = raise_for_status
        self.content = json.dumps(self._json_data)
        self.text = json.dumps(self._json_data)

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self._raise_for_status:
            raise requests.HTTPError("Mocker Error")
        return self.status_code


def get_mock_response(status_code=200, raise_for_status=False, json_data=None, headers=None):
    return MockResponse(status_code, headers=headers, json_data=json_data, raise_for_status=raise_for_status)


class TestFrontAppClient(unittest.TestCase):

    @patch("requests.request")
    def test_successful_request(self, mock_request):
        mock_request.return_value = get_mock_response(
            status_code=200,
            headers={"X-Ratelimit-Remaining": "100", "X-Ratelimit-Reset": "1000"},
            json_data={"metrics": [{"id": "m1"}]}
        )
        client = Client(config={"token": "test-token"})
        response = client.get_report_metrics("https://api2.frontapp.com/analytics/reports/xyz")
        self.assertEqual(response, [{"id": "m1"}])

    @patch("requests.request")
    def test_rate_limit_429(self, mock_request):
        mock_request.return_value = get_mock_response(
            status_code=429,
            headers={"X-Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "999"},
            raise_for_status=True
        )
        client = Client(config={"token": "test-token"})
        with self.assertRaises(RateLimitException):
            client.get_report_metrics("https://api2.frontapp.com/analytics/reports/xyz")

    @patch("requests.request")
    def test_metrics_rate_limit_423(self, mock_request):
        mock_request.return_value = get_mock_response(
            status_code=423,
            headers={"X-Ratelimit-Remaining": "10", "X-Ratelimit-Reset": "999"},
            raise_for_status=True
        )
        client = Client(config={"token": "test-token"})
        with self.assertRaises(MetricsRateLimitException):
            client.get_report_metrics("https://api2.frontapp.com/analytics/reports/xyz")

    @patch("requests.request", side_effect=Timeout)
    def test_timeout_handling(self, mock_request):
        client = Client(config={"token": "test-token"})
        with self.assertRaises(Timeout):
            client.get_report_metrics("https://api2.frontapp.com/analytics/reports/xyz")

    @patch("requests.request", side_effect=ConnectionError)
    def test_connection_error_handling(self, mock_request):
        client = Client(config={"token": "test-token"})
        with self.assertRaises(ConnectionError):
            client.get_report_metrics("https://api2.frontapp.com/analytics/reports/xyz")
