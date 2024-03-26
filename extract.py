"""Functions which help extract data from an S3 bucket."""

from os import _Environ

import xml.etree.ElementTree as ET
import pandas as pd
from mypy_boto3_s3 import S3Client
from boto3 import client


def get_s3_client(config: _Environ) -> S3Client:
    """Returns an S3 client object."""

    return client(
            's3',
            aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY']
            )


def get_bucket_objs(s3_client: S3Client, bucket: str) -> list:
    """Returns a list of objects from an S3 bucket."""

    contents = s3_client.list_objects(Bucket=bucket)["Contents"]

    return [obj for obj in contents if obj["Key"].startswith("c9-zander-input")]


def get_latest_xml_data(s3_client: S3Client, bucket: str) -> ET.ElementTree:
    """Returns the XML data from the most recent file in an S3 bucket."""

    objects = get_bucket_objs(s3_client, bucket)

    latest_key = sorted(objects, key=lambda x: x['LastModified'], reverse=True)[0]['Key']

    latest_obj = s3_client.get_object(Bucket=bucket, Key=latest_key)

    latest_obj_body = latest_obj['Body'].read().decode()

    return ET.fromstring(latest_obj_body)


def get_pubmed_articles(data: ET.ElementTree) -> list:
    """Returns a list of the PubmedArticles only."""

    return data.findall(".//PubmedArticle")


def get_basic_article_info(article: ET.Element) -> dict:
    """Returns a dictionary of information about the given article."""

    title = article.find("./MedlineCitation/Article/ArticleTitle").text

    try:
        year = article.find("./MedlineCitation/Article/Journal/JournalIssue/PubDate/Year").text
    except AttributeError:
        year = None


    pmid = article.find("./MedlineCitation/PMID").text

    try:
        keyword_list = [k.text for k in article.find("./MedlineCitation/KeywordList")]
    except TypeError:
        keyword_list = None

    try:
        mesh_list = [
            h.find("./DescriptorName").attrib.get("UI")
            for h in article.find("./MedlineCitation/MeshHeadingList")
            ]
    except TypeError:
        mesh_list = None

    return {
        "title": title,
        "pmid": pmid,
        "year": year,
        "keyword_list": keyword_list,
        "mesh_list": mesh_list
    }


def get_author_info(author: ET.Element) -> dict:
    """Returns a dictionary of information about the given author."""

    try:
        forename = author.find("./ForeName").text
    except AttributeError:
        forename = None

    try:
        lastname = author.find("./LastName").text
    except AttributeError:
        lastname = None

    try:
        initials = author.find("./Initials").text
    except AttributeError:
        initials = None

    try:
        identity = author.find("./AffiliationInfo/Identifier[@Source='GRID']").text
    except AttributeError:
        identity = None

    affiliation = [a.find("./Affiliation").text for a in author.findall("./AffiliationInfo")]

    return {
        "forename": forename,
        "lastname": lastname,
        "initials": initials,
        "identity": identity,
        "affiliation": affiliation
    }


def get_authors(article: ET.Element) -> list:
    """Returns a list of author elements from an article."""

    return article.findall("./MedlineCitation/Article/AuthorList/Author")


def get_all_article_data(article: ET.Element) -> list:
    """Returns a list of dictionaries of all the relevant information for a given article."""

    data = []
    authors = get_authors(article)
    for author in authors:
        base = get_basic_article_info(article)
        info = base | get_author_info(author)
        data.append(info)

    return data


def get_dataframe_from_articles(articles: list) -> pd.DataFrame:
    """Returns a dataframe of all the relevant information for each article."""

    data = []
    for article in articles:
        data += get_all_article_data(article)

    return pd.DataFrame(data).explode("affiliation")
