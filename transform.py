"""Functions which help transform the data."""

import logging

import pandas as pd
import country_converter as coco
from rapidfuzz.distance import Levenshtein
from rapidfuzz.process import extractOne
import spacy

from comp_data import get_country_names, get_institutes_data


BRITISH_POSTCODE_REGEX = r"([A-Z0-9]{2,4} [0-9][A-Z]{2})\b"
AMERICAN_POSTCODE_REGEX = r"\b[A-Z]{2} ([0-9]{5})\b"
CANADIAN_POSTCODE_REGEX = r"\b([A-Z][0-9][A-Z] [0-9][A-Z][0-9])\b"
EMAIL_REGEX = r"([a-z0-9\.-]+@[a-z0-9\.-]+)\."

SIMILARITY_CUTOFF = 0.9

COUNTRY_NAMES = get_country_names()
INST_DATA = get_institutes_data()

nlp = spacy.load("en_core_web_sm")


def split_affiliations(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Splits the affiliation data into unique affiliations."""

    dataframe["affiliation"] = dataframe['affiliation'].apply(lambda x: str(x).split(';'))

    dataframe = dataframe.explode('affiliation')

    dataframe['affiliation'] = dataframe['affiliation'].replace("nan", None)

    return dataframe


def add_email_to_dataframe(dataframe: pd.DataFrame) -> None:
    """Adds an email column to the dataframe."""

    dataframe['email'] = dataframe['affiliation'].str.extract(EMAIL_REGEX)


def add_postcodes_to_dataframe(dataframe: pd.DataFrame) -> None:
    """Adds postcodes to dataframe."""

    british = dataframe['affiliation'].str.extract(BRITISH_POSTCODE_REGEX)
    american = dataframe['affiliation'].str.extract(AMERICAN_POSTCODE_REGEX)
    canadian = dataframe['affiliation'].str.extract(CANADIAN_POSTCODE_REGEX)

    british.update(american)
    british.update(canadian)

    dataframe['postcode'] = british

def extract_country_and_org(row: pd.Series) -> pd.Series:
    """Extracts the country and organisation from the affiliation string."""

    logging.getLogger("country_converter").setLevel(logging.ERROR)

    if not row['affiliation']:
        return row

    country_con = coco.CountryConverter()

    orgs_and_countries = [item for item in list(nlp(str(row["affiliation"])).ents)
                          if item.label_ in ("GPE", "ORG")]
    poss_countries = [country.text for country in orgs_and_countries if country.label_ == "GPE"]

    if not poss_countries:
        row['country'] = None
    else:
        likely_country = poss_countries[-1]
        likely_country = country_con.convert(str(likely_country), to="short_name")
        if str(likely_country) in COUNTRY_NAMES:
            row['country'] = str(likely_country)
        else:
            row['country'] = None

    orgs = [org.text for org in orgs_and_countries if org.label_ == "ORG"]

    if not orgs:
        row['organisation'] = None
    else:
        row['organisation'] = orgs[-1].split(", ")[-1]

    return row


def get_match(row: pd.Series) -> pd.Series:
    """Gets the organisation name and GRID ID if found in the GRID institutes data."""

    if not row['organisation']:
        return row

    if row['country']:
        comp_data = INST_DATA[INST_DATA['country'] == row['country']]
    else:
        comp_data = INST_DATA

    match_org = extractOne(
        row['organisation'],
        comp_data['name'],
        scorer=Levenshtein.normalized_similarity,
        score_cutoff=SIMILARITY_CUTOFF
        )

    if match_org:
        match_org = match_org[0]

        match_id = comp_data.loc[comp_data['name'] == match_org, 'grid_id'].iloc[0]

        row['organisation'] = match_org
        row['identity'] = match_id
        return row

    row['organisation'] = None
    row['identity'] = None

    return row
