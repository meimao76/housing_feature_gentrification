import pandas as pd

def get_residential_units(es, year, index="applications"):
    """
    Retrieve specified columns for all entries where total_no_proposed_residential_units >= 1
    and decision_date is within a given year.

    Args:
        es (obj): Elasticsearch connection object.
        year (int): The year for which decision_date should be filtered.
        index (str, optional): Elasticsearch index name. Defaults to "applications".

    Returns:
        DataFrame: A Pandas DataFrame with the filtered applications.
    """
    
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "application_details.residential_details.total_no_existing_residential_units": {
                                "gte": 1
                            }
                        }
                    },
                    {
                        "range": {
                            "decision_date": {
                                "gte": f"01/01/{year}",
                                "lt": f"01/01/{year + 1}"
                            }
                        }
                    }
                ]
            }
        },
        "_source": [
            "lpa_app_no",
            "valid_date",
            "decision_date",
            "borough",
            "application_details.residential_details.total_no_proposed_residential_units",
            "application_details.residential_details.total_no_affordable_units",
            "application_details.residential_details.site_area",
            "application_details.residential_details.habitable_rooms_density",
            "pp_id",
            "uprn",
            "status",
            "decision",
            "street_name",
            "site_name",
            "site_number",
            "polygon",
            "wgs84_polygon",
            "description"
        ]
    }
    
    # Initialize scrolling
    response = es.search(index=index, body=query, scroll="2m", size=10000)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    
    all_hits = []
    all_hits.extend(hits)

    # Keep scrolling until no hits are returned
    while len(hits) > 0:
        response = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        all_hits.extend(hits)

    # Normalize the results into a DataFrame
    df = pd.json_normalize(all_hits)
    
    return df



def get_residential_units_by_borough(es, borough, year, index="applications"):
    """
    Retrieve specified columns for all entries where total_no_proposed_residential_units >= 1,
    the borough matches the given value, and decision_date is within a given year.

    Args:
        es (obj): Elasticsearch connection object.
        borough (str): The borough for which data should be filtered.
        year (int): The year for which decision_date should be filtered.
        index (str, optional): Elasticsearch index name. Defaults to "applications".

    Returns:
        DataFrame: A Pandas DataFrame with the filtered applications.
    """
    
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "application_details.residential_details.total_no_proposed_residential_units": {
                                "gte": 1
                            }
                        }
                    },
                    {
                        "term": {
                            "borough": borough
                        }
                    },
                    {
                        "range": {
                            "decision_date": {
                                "gte": f"01/01/{year}",
                                "lt": f"01/01/{year + 1}"
                            }
                        }
                    }
                ]
            }
        },
        "_source": [
            "lpa_app_no",
            "valid_date",
            "decision_date",
            "borough",
            "application_details.residential_details.total_no_proposed_residential_units",
            "application_details.residential_details.total_no_affordable_units",
            "application_details.residential_details.site_area",
            "application_details.residential_details.habitable_rooms_density",
            "pp_id",
            "uprn",
            "status",
            "decision",
            "street_name",
            "site_name",
            "site_number",
            "polygon",
            "wgs84_polygon",
            "description"
        ]
    }
    
    # Initialize scrolling
    response = es.search(index=index, body=query, scroll="2m", size=10000)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']
    
    all_hits = []
    all_hits.extend(hits)

    # Keep scrolling until no hits are returned
    while len(hits) > 0:
        response = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        all_hits.extend(hits)

    # Normalize the results into a DataFrame
    df = pd.json_normalize(all_hits)
    
    return df