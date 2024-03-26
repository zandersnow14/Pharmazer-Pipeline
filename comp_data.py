"""Functions which create datasets to be used in the pipeline."""

import geonamescache
import pandas as pd

def get_country_names() -> list:
    """Returns a list of standardised country names."""

    geo_cache = geonamescache.GeonamesCache()
    countries = geo_cache.get_countries()
    return [v['name'] for k, v in countries.items()]


def get_institutes_data() -> pd.DataFrame:
    """
    Returns a dataframe containing the relevant information on every institution in the GRID 
    dataset.
    """

    institutes = pd.read_csv("grid_data/institutes.csv")
    addresses = pd.read_csv("grid_data/addresses.csv", dtype=str)
    return pd.merge(institutes, addresses, on='grid_id')[['grid_id', 'name', 'country']]
