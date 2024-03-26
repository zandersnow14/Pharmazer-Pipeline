"""Script which extracts data from an XML file and outputs it to a CSV."""

from os import environ, _Environ
from datetime import datetime
from time import perf_counter

import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from extract import get_latest_xml_data, get_pubmed_articles, get_dataframe_from_articles
from extract import get_s3_client
from transform import split_affiliations, add_email_to_dataframe, add_postcodes_to_dataframe
from transform import get_match, extract_country_and_org

OUTPUT_FOLDER = "c9-zander-output"
CURRENT_TIME = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def extract(config: _Environ, bucket: str) -> pd.DataFrame:
    """Returns a dataframe with basic information relating to articles."""

    s3_client = get_s3_client(config)
    data = get_latest_xml_data(s3_client, bucket)
    articles = get_pubmed_articles(data)

    return get_dataframe_from_articles(articles)

def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Transforms the data by adding various columns with extra details."""

    dataframe = split_affiliations(dataframe)

    add_email_to_dataframe(dataframe)
    add_postcodes_to_dataframe(dataframe)

    dataframe = dataframe.parallel_apply(extract_country_and_org, axis=1)
    dataframe = dataframe.parallel_apply(get_match, axis=1)

    dataframe = dataframe[
        ['pmid', 'title', 'keyword_list', 'mesh_list', 'year', 'forename', 'lastname',
         'initials', 'email', 'affiliation', 'organisation', 'postcode', 'country', 'identity']
         ]

    return dataframe


if __name__ == "__main__":

    start = perf_counter()
    load_dotenv()
    pandarallel.initialize()

    pubmed_df = extract(environ, environ['INPUT_BUCKET'])
    pubmed_df = transform(pubmed_df)
    pubmed_df.to_csv(
        f"s3://{environ['OUTPUT_BUCKET']}/{OUTPUT_FOLDER}/{CURRENT_TIME}.csv", index=False
        )

    total_time = round(perf_counter() - start, 2)
    print(f"Time taken: {total_time} secs")
