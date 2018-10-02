import requests
import os
import logging


logging.basicConfig(level="INFO")


def get_headlines(**context):
    """
    takes a list of key words and querys the newsapi for each one.
    All headlines are returned as a dictionary keyed by the word that
    was queryed.
    params
        :context(dict) -> A key word mapping of task information.
            :params(dict) -> a dict of paramters to be used in this task
                :base_url(string) -> base url for the newsapi.
                :word_list(list) -> the list of keywords to query the api for.
    return
        :response_data(dict) -> a dictionary containining the result from each
                                query keyed by the word sent to the api
    """
    key_words = context["params"]["word_list"]
    base_url = context["params"]["url"]
    api_key = os.environ["API_KEY"]
    headers = {"X-API-KEY": api_key}
    response_data = {}
    exception_count = 0
    for word in key_words:
        try:
            query = base_url + word
            r = requests.get(query, headers=headers)
            r.raise_for_status()
            response_data[word] = r.json().get("articles", )
            logging.info("response for {} added to response data".format(word))
        except Exception as e:
            exception_count += 1
            logging.exception(e)
            continue
    if exception_count == len(key_words):
        raise Exception  # raise exception if all api calls failed
    return response_data
