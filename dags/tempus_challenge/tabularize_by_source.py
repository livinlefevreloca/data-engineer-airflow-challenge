import csv
import os
import logging
import shutil

from values import csv_header

logging.basicConfig(level='INFO')


def tabularize(**context) -> str:
    """
    A function that takes json data from the newsapi and writes it in a
    flattened tabular format to csv files.  Each source is a single file
    containing all headlines from it for the day.
    params
        :context(dict) -> key word mapping of task information
    return
        :dir_name(string) -> the name of the directory the csvs are stored in

    possible_failures:
        if for some reason the directory "csv_temp" exists the error is logged
        and the program continues.  This could occur if the final task does not
        execute to completion and remove the directory.
    """
    # get the json data from the news api returned by the previous task
    headlines = context["ti"].xcom_pull(task_ids='source_api')
    source_map = flatten_data(headlines)

    try:
        csv_dir = "csv_temp_source"
        os.makedirs(csv_dir)
    except FileExistsError as e:
        logging.exception(e)
        # remove existing directory that wasnt cleaned up and make a new one.
        shutil.rmtree(csv_dir)
        os.makedirs(csv_dir)

    for key, value in source_map.items():
        filename = key + ".csv"
        with open(os.path.join(csv_dir, filename), 'w') as fh:
            writer = csv.DictWriter(fh, fieldnames=csv_header)
            writer.writeheader()
            for headline in value:
                writer.writerow(headline)

    return csv_dir


def flatten_data(headlines):
    """
    A function to transform the json data from the news api into a dictionary
    with sources as keys and api data as the value
    params
        :headlines(dict) -> a dictionary of the english source headlines
    return
        :source_map(dict) -> a dictionary mapping sources to their headlines of
        the day.
    """
    source_map = {}

    logging.info("attempting to flatten headlines")
    for headline in headlines:
        source = headline["source"]
        del headline["source"]
        headline["source_name"] = source["name"]
        headline["source_id"] = source["id"]
        if source["id"] in source_map.keys():
            source_map[source["id"]].append(headline)
            continue

        source_map[source["id"]] = [headline, ]

    logging.info("headlines successfully flattened")

    return source_map
