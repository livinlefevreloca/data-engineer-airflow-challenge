import pytest
import time
import os
import requests

import dags.news_pipeline.values as vals
from dags import news_pipeline as n


class TestTabularize:

    @pytest.fixture
    def api_data(self, retries=0):
        api_key = os.environ["API_KEY"]
        headers = {"X-API-Key": api_key}
        url = "https://newsapi.org/v2/top-headlines?language=en"
        r = requests.get(url, headers=headers)
        try:
            r.raise_for_status()
        except Exception as e:
            # try 5 times to get response from api. if now raise exception
            if retries > 5:
                raise
            time.sleep(2)
            return self.api_data(retries+1).get("articles")
        return r.json().get("articles")

    def test_flatten_data(self, api_data):
        sources = [data["source"]["id"] for data in api_data]
        source_map = n.flatten_data(api_data)
        # the csv header should be the keys for each source dict in source_map
        for source in source_map.values():
            for headline in source:
                assert sorted(headline.keys()) == sorted(vals.csv_header)
        # all sources returned by the api call should be in the source_map
        assert all([source in source_map for source in sources])
