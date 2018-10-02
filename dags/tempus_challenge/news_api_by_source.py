import requests
import os
import logging


logging.basicConfig(level='INFO')


def get_headlines(**context):
    """
    A function to retrieve the headlines from all english news sources
    using the news api.
    params
        :context(dict) -> A key word mapping of task information.
            :params(dict) -> a dict of paramters to be used in this task
                :base_url(string) -> base url for the newsapi.
                :query(string) -> the query sent to newsapi to retrieve all
                                  english sources.
    return
        res_data(dict) -> json data for the major english headlines of the day.

    possible_failures:
        - Bad response is handled by DAG retries. the response is checked for
          by "r.raise_for_status". If it fails the exception is logged and then
          raised again so the DAG will retry the task.
    """
    url = context["params"]["url"]

    try:
        api_key = os.environ["API_KEY"]
        headers = {"X-Api-Key": api_key}
        r = requests.get(url, headers=headers)
        r.raise_for_status()  # ensure API call was successful
        res_data = r.json()
        return res_data.get("articles")
    except Exception as e:
        logging.exception(e)
        raise
