import os
import csv
import logging
import shutil

from values import csv_header

logging.basicConfig(level="INFO")


def tabularize(**context):
    """
    transform key word data from the news api and tabularize it in csv files.
    params
        params
            :context(dict) -> key word mapping of task information
        return
            :csv_dir(string) -> the name of the directory containg the csvs
    """
    data = context["ti"].xcom_pull(task_ids="keyword_api")
    try:
        csv_dir = "csv_temp_keyword"
        os.makedirs(csv_dir)
    except FileExistsError as e:
        logging.exception(e)
        # remove existing directory that wasnt cleaned up and make a new one.
        shutil.rmtree(csv_dir)
        os.makedirs(csv_dir)

    try:
        for key, value in data.items():
            flattened_data = flatten_data(value)
            write_to_csv(key, flattened_data, csv_dir)
        return csv_dir
    except Exception as e:
        logging.exception(e)
        raise


def flatten_data(headlines):
    """
    Flatten each individual headline related to a certain keyword.
    params
        :headlines(list) -> A list of headlines returned by the newsapi.
    return
        :flattened(list) -> A list of flattened headline dicts.
    """
    flattened = []
    for headline in headlines:
        source = headline["source"]
        del headline["source"]
        headline["source_id"] = source["id"]
        headline["source_name"] = source["name"]
        flattened.append(headline)
    return flattened


def write_to_csv(keyword, headlines, csv_dir):
    """
    Write a set of headlines specfic to a keyword a single csv
    params
        :keyword(string) -> the keyword that was used to retrieve the headlines
                            from the newsapi.
        :headlines(list) -> A list of dicts containing data pertaining to a
                            specfic headline retrieved from the newsapi.
        :csv_dr(string) -> The temporary directory to store the csvs
    """
    file_name = os.path.join(csv_dir, keyword+".csv")
    with open(file_name, "w") as fh:
        writer = csv.DictWriter(fh, fieldnames=csv_header)
        writer.writeheader()
        for headline in headlines:
            writer.writerow(headline)
