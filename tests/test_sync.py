import unittest
import unittest.mock
import urllib.parse

import pendulum
import requests_mock

from tap_marketo.client import Client, ApiException
from tap_marketo.discover import discover_catalog
from tap_marketo.sync import *


def parse_params(request):
    return dict(urllib.parse.parse_qsl(request.query))


class TestSyncActivityTypes(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = discover_catalog("activity_types")
        for schema in self.stream["schema"]["properties"].values():
            schema["selected"] = True

    @unittest.mock.patch("singer.write_record")
    def test_sync_activity_types(self, write_record):
        activity_type = {
            "id": 1,
            "name": "Visit Webpage",
            "description": "User visits a web page",
            "primaryAttribute": {
                "name": "Webpage ID",
                "dataType": "integer",
            },
            "attributes": [
                {
                    "name": "Client IP Address",
                    "dataType": "string",
                },
                {
                    "name": "Query Parameters",
                    "dataType": "string",
                },
            ],
        }

        data = {
            "success": True,
            "result": [activity_type],
        }

        with requests_mock.Mocker(real_http=True) as mock:
            mock.register_uri("GET", self.client.get_url("rest/v1/activities/types.json"), json=data)
            sync_activity_types(self.client, {}, self.stream)

        write_record.assert_called_once_with("activity_types", activity_type)


class TestSyncPaginated(unittest.TestCase):
    def setUp(self):
        self.client = Client("123-ABC-456", "id", "secret")
        self.client.token_expires = pendulum.utcnow().add(days=1)
        self.client.calls_today = 1
        self.stream = discover_catalog("programs")

    def test_sync_paginated(self):
        state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
        endpoint = "rest/v1/programs.json"
        url = self.client.get_url(endpoint)

        responses = [
            {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
            {"json": {"success": True, "result": []}, "status_code": 200},
        ]

        with requests_mock.Mocker(real_http=True) as mock:
            matcher = mock.register_uri("GET", url, responses)
            sync_paginated(self.client, state, self.stream)

        # We should have made 2 requests
        self.assertEqual(2, len(matcher.request_history))

        # Assert that we used the paging token from state
        self.assertDictEqual({"batchsize": "300", "nextpagetoken": "abc"}, parse_params(matcher.request_history[0]))
        self.assertDictEqual({"batchsize": "300", "nextpagetoken": "def"}, parse_params(matcher.request_history[1]))

    def test_sync_paginated_fail(self):
        state = {"bookmarks": {"programs": {"updatedAt": "2017-01-01T00:00:00Z", "next_page_token": "abc"}}}
        endpoint = "rest/v1/programs.json"
        url = self.client.get_url(endpoint)

        # The empty 200 is an actual Marketo response to bad requests
        responses = [
            {"json": {"success": True, "result": [], "nextPageToken": "def"}, "status_code": 200},
            {"text": "", "status_code": 200},
        ]

        with requests_mock.Mocker(real_http=True) as mock:
            matcher = mock.register_uri("GET", url, responses)
            with self.assertRaises(ApiException):
                sync_paginated(self.client, state, self.stream)

        # The last paging token should still be there
        self.assertEqual("def", state["bookmarks"]["programs"]["next_page_token"])


class TestSyncActivities(unittest.TestCase):
    # TODO: need to test the waiting for export stuff
    # TODO: need to test flattening
    # TODO: test get or create export
    pass