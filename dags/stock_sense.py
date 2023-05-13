import logging
import json
from urllib import request
from airflow.utils import dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator



def _get_wikimedia_data(output_path, execution_date, **context):
    year, month, day, hour, *_ = execution_date.timetuple()
    hour -= 2
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )

    logging.info(f"URL: {url}")
    logging.info(f"output: {output_path}")

    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, page_views_path, **context):
    result = dict.fromkeys(pagenames, 0)
    with open(page_views_path, "r") as f:
        for line in f:
            domain_code, page_title, views_count, _ = line.split(" ")
            if domain_code == "en" and page_title.lower() in pagenames:
                result[page_title.lower()] += int(views_count)

    logging.info(json.dumps(result, indent=2))
    
    execution_date = context["execution_date"]
    with open("/tmp/view_count.sql", "w") as f:
        for page_name, count in result.items():
            statement = f"INSERT INTO page_view_counts VALUES ('{page_name}', {count}, '{execution_date}');"
            logging.info(statement)
            f.write(statement)


with DAG(
    dag_id="stock_sense",
    start_date=dates.days_ago(3),
    schedule_interval="@hourly",
    catchup=True,
    template_searchpath="/tmp"
) as dag:
    output_path = "/tmp/wikipageviews.gz"

    get_wikimedia_data = PythonOperator(
        task_id="get_wikimedia_data",
        python_callable=_get_wikimedia_data,
        op_kwargs={"output_path": output_path},
    )

    extract_gz = BashOperator(
        task_id="extract_gz", bash_command=f"gunzip --force {output_path}"
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames": {"google", "microsoft", "apple", "amazon", "chatgpt"},
            "page_views_path": "/tmp/wikipageviews",
        },
    )
    
    create_table_if_not_exist = PostgresOperator(
        task_id="create_table_if_not_exist",
        postgres_conn_id="my_postgres",
        sql= """
        CREATE TABLE IF NOT EXISTS page_view_counts (
            page_name VARCHAR(50) NOT NULL,
            view_count INT NOT NULL,
            datetime TIMESTAMP NOT NULL
        );
        """
    )
    
    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        postgres_conn_id="my_postgres",
        sql="view_count.sql"
    )
    get_wikimedia_data >> extract_gz >> fetch_pageviews >> create_table_if_not_exist >> write_to_postgres

#airflow connections add --conn-type postgres --conn-host postgres --conn-login airflow --conn-password airflow my_postgres