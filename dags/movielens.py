import json
import logging
import os

import requests
import pandas as pd
from airflow import DAG
from airflow.utils import dates
from airflow.operators.python import PythonOperator


MOVIESLENS_HOST = os.environ.get("MOVIESLENS_HOST", "movielens")
MOVIESLENS_SCHEMA = os.environ.get("MOVIESLENS_SCHEMA", "http")
MOVIESLENS_PORT = os.environ.get("MOVIESLENS_PORT", "5000")
MOVIESLENS_USER = os.environ["MOVIESLENS_USER"]
MOVIESLENS_PASSWORD = os.environ["MOVIESLENS_PASSWORD"]


def _get_session():
    """Builds a request session for MovielensAPI"""
    session = requests.Session()
    session.auth = (MOVIESLENS_USER, MOVIESLENS_PASSWORD)
    base_url = f"{MOVIESLENS_SCHEMA}://{MOVIESLENS_HOST}:{MOVIESLENS_PORT}"
    return session, base_url


def _get_with_pagination(session, url, params, batch_size=100):
    """Fetches records using GET request with given URL/params
    Taking pagination into account
    """
    logger = logging.getLogger(__name__)
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            url, params={**params, **{"offset": offset, "limit": batch_size}}
        )
        logger.info(f"URL={response.url}")
        response.raise_for_status()
        response_json = response.json()

        yield from response_json["result"]

        offset += batch_size
        total = response_json["total"]


def _get_ratings(start_date, end_date, batch_size=100):
    logger = logging.getLogger(__name__)
    
    session, base_url = _get_session()
    logger.info(base_url)
    yield from _get_with_pagination(
        session=session,
        url=base_url + "/ratings",
        params={"start_date": start_date, "end_date": end_date},
        batch_size=batch_size,
    )


def _fetch_ratings(templates_dict, batch_size=1000):
    logger = logging.getLogger(__name__)

    start_date = templates_dict["start_date"]
    end_date = templates_dict["end_date"]
    output_path = templates_dict["output_path"]

    logger.info(f"Fetching ratings from {start_date} to {end_date}")
    ratings = list(
        _get_ratings(start_date=start_date, end_date=end_date, batch_size=batch_size)
    )
    logger.info(f"Fetched {len(ratings)} ratings.")
    logger.info(f"Writing ratings to {output_path}")

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "w") as file_:
        json.dump(ratings, file_)


def rank_movies_by_rating(ratings, min_ratings=2):
    ranking = (
        ratings.groupby("movie_id")
        .agg(
            avg_rating=pd.NamedAgg(column="rating", aggfunc="mean"),
            num_ratings=pd.NamedAgg(column="userId", aggfunc="nunique"),
        )
        .loc[lambda df: df["num_ratings"] > min_ratings]
        .sort_values(["avg_rating", "num_ratings"], ascending=False)
    )
    return ranking

def _rank_movies(templates_dict, min_ratings=2):
    input_path = templates_dict["input_path"]
    output_path = templates_dict["output_path"]
    
    ratings = pd.read_json(input_path)
    ranking = rank_movies_by_rating(ratings, min_ratings)
    
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    ranking.to_csv(output_path, index=True)

with DAG(
    dag_id="MovieLensRatings",
    start_date=dates.days_ago(3),
    schedule_interval="@daily",
    catchup=False
):  
    fetch_ratings = PythonOperator(
        task_id="fetch_rating",
        python_callable=_fetch_ratings,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/ratings/{{ds}}.json"
        }
    )
    rank_movies = PythonOperator(
        task_id="rank_movies",
        templates_dict={
            "input_path": "/data/ratings/{{ds}}.json",
            "output_path": "/data/rankings/{{ds}}.json"
        },
        python_callable=_rank_movies
    )
    
    fetch_ratings >> rank_movies
                