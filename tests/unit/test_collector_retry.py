# pyright: reportPrivateUsage=false
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests

from collectors.base import CollectorError
from collectors.naver_land_collector import NaverLandCollector
from collectors.onbid_collector import OnbidCollector
from collectors.subscription_collector import SubscriptionCollector


class _JsonResponse:
    def __init__(self, payload: dict[str, object], error: Exception | None = None) -> None:
        self._payload: dict[str, object] = payload
        self._error: Exception | None = error

    def raise_for_status(self) -> None:
        if self._error:
            raise self._error

    def json(self) -> dict[str, object]:
        return self._payload


class _HtmlResponse:
    def __init__(self, text: str, error: Exception | None = None) -> None:
        self.text: str = text
        self.encoding: str = "utf-8"
        self._error: Exception | None = error

    def raise_for_status(self) -> None:
        if self._error:
            raise self._error


class TestOnbidCollectorRetry:
    @staticmethod
    def _create_collector() -> OnbidCollector:
        return OnbidCollector("onbid_test", {"api_key": "test_key"})

    @patch("collectors.onbid_collector.requests.get")
    def test_retry_on_timeout(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.Timeout("timeout"),
            requests.exceptions.Timeout("timeout"),
            _JsonResponse({"response": {"body": {"items": []}}}),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._make_request("endpoint", {"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 3

    @patch("collectors.onbid_collector.requests.get")
    def test_retry_on_5xx_error(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        fail_response = _JsonResponse(
            {"response": {"body": {"items": []}}},
            error=requests.exceptions.HTTPError("500 Server Error"),
        )
        success_response = _JsonResponse({"response": {"body": {"items": []}}})

        mock_get.side_effect = [fail_response, fail_response, success_response]

        with patch("time.sleep", return_value=None):
            result = collector._make_request("endpoint", {"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 3

    @patch("collectors.onbid_collector.requests.get")
    def test_max_retries_exceeded(self, mock_get: Mock) -> None:
        collector = self._create_collector()
        mock_get.side_effect = requests.exceptions.Timeout("timeout")

        with patch("time.sleep", return_value=None):
            with pytest.raises(CollectorError, match="Onbid API request failed"):
                _ = collector._make_request("endpoint", {"pageNo": 1})

        assert mock_get.call_count == 3

    @patch("collectors.onbid_collector.requests.get")
    def test_connection_error_recovery(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection error"),
            _JsonResponse({"response": {"body": {"items": []}}}),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._make_request("endpoint", {"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 2


class TestNaverLandCollectorRetry:
    @staticmethod
    def _create_collector() -> NaverLandCollector:
        return NaverLandCollector("naver_land_test", {"timeout": 1})

    @patch("collectors.naver_land_collector.requests.get")
    def test_retry_on_timeout(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.Timeout("timeout"),
            requests.exceptions.Timeout("timeout"),
            _HtmlResponse("<html>ok</html>"),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._fetch_html("https://land.naver.com/search/result?page=1")

        assert result == "<html>ok</html>"
        assert mock_get.call_count == 3

    @patch("collectors.naver_land_collector.requests.get")
    def test_retry_on_5xx_error(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        fail_response = _HtmlResponse(
            "<html>fail</html>",
            error=requests.exceptions.HTTPError("500 Server Error"),
        )
        success_response = _HtmlResponse("<html>ok</html>")
        mock_get.side_effect = [fail_response, fail_response, success_response]

        with patch("time.sleep", return_value=None):
            result = collector._fetch_html("https://land.naver.com/search/result?page=1")

        assert result == "<html>ok</html>"
        assert mock_get.call_count == 3

    @patch("collectors.naver_land_collector.requests.get")
    def test_max_retries_exceeded(self, mock_get: Mock) -> None:
        collector = self._create_collector()
        mock_get.side_effect = requests.exceptions.Timeout("timeout")

        with patch("time.sleep", return_value=None):
            with pytest.raises(requests.exceptions.Timeout):
                _ = collector._fetch_html("https://land.naver.com/search/result?page=1")

        assert mock_get.call_count == 3

    @patch("collectors.naver_land_collector.requests.get")
    def test_connection_error_recovery(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection error"),
            _HtmlResponse("<html>ok</html>"),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._fetch_html("https://land.naver.com/search/result?page=1")

        assert result == "<html>ok</html>"
        assert mock_get.call_count == 2


class TestSubscriptionCollectorRetry:
    @staticmethod
    def _create_collector() -> SubscriptionCollector:
        return SubscriptionCollector("subscription_test", {"api_key": "test_key"})

    @patch("collectors.subscription_collector.requests.get")
    def test_retry_on_timeout(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.Timeout("timeout"),
            requests.exceptions.Timeout("timeout"),
            _JsonResponse({"response": {"body": {"items": []}}}),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._make_request({"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 3

    @patch("collectors.subscription_collector.requests.get")
    def test_retry_on_5xx_error(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        fail_response = _JsonResponse(
            {"response": {"body": {"items": []}}},
            error=requests.exceptions.HTTPError("500 Server Error"),
        )
        success_response = _JsonResponse({"response": {"body": {"items": []}}})
        mock_get.side_effect = [fail_response, fail_response, success_response]

        with patch("time.sleep", return_value=None):
            result = collector._make_request({"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 3

    @patch("collectors.subscription_collector.requests.get")
    def test_max_retries_exceeded(self, mock_get: Mock) -> None:
        collector = self._create_collector()
        mock_get.side_effect = requests.exceptions.Timeout("timeout")

        with patch("time.sleep", return_value=None):
            with pytest.raises(CollectorError, match="Subscription API request failed"):
                _ = collector._make_request({"pageNo": 1})

        assert mock_get.call_count == 3

    @patch("collectors.subscription_collector.requests.get")
    def test_connection_error_recovery(self, mock_get: Mock) -> None:
        collector = self._create_collector()

        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection error"),
            _JsonResponse({"response": {"body": {"items": []}}}),
        ]

        with patch("time.sleep", return_value=None):
            result = collector._make_request({"pageNo": 1})

        assert result == {"response": {"body": {"items": []}}}
        assert mock_get.call_count == 2
