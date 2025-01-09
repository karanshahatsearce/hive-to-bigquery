import re
import sys
import pip

print(f"Python executable: {sys.executable}")
print(f"Pip executable: {pip.__file__}")
import pandas as pd
import findspark
import logging


findspark.init("/Users/karanshah/usr/lib/spark")
from pyhive import hive
from google.cloud import bigquery, storage
import json
import subprocess
from subprocess import check_call
from timeit import default_timer as timer
import threading
from multiprocessing.pool import ThreadPool as Pool
import os

start = timer()


def distcp(from_path: str, to_path: str) -> None:
    print(f"from_path:{from_path}, to_path: {to_path}")
    check_call(
        ["hadoop", "distcp", "-direct", from_path, to_path], stderr=subprocess.STDOUT
    )
    print("=====================HIVE TO GCS======================================")
    print(f"Added {to_path} to GCS")


def worker(element):
    try:
        create_tables(element)
    except:
        print(f"error with {element}")


def create_tables(element):
    project = os.environ.get("project")
    bucket_name = os.environ.get("bucket_name")
    print("Create table is initialised")
    # migrating hive to gcs
    from_path = os.environ.get("from_path")
    #       from_path="hdfs://accelerator-hive-bq-m/user/hive/warehouse/hivedemo.db/"
    from_path = from_path + element
    to_path = os.environ.get("to_path")
    #       to_path="gs://demo-gcs-bq"
    distcp(from_path, to_path)
    # migrating gcs to bq
    client = bigquery.Client(project)
    bucket = storage.Client().bucket(bucket_name)
    # Construct a BigQuery client object.
    table_id = os.environ.get("table_id")
    table_id1 = table_id + element + "_extra"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.ORC,
        autodetect=True,
    )
    prefix1 = element + "/"
    # print("Prefix ",prefix)
    for blob in bucket.list_blobs(prefix=prefix1):
        filename = blob.name
        print("filename: ", filename)
        if not "_orc_acid_version" in blob.name:
            table_name = re.findall(r"([a-z|_|A-Z|0-9]+).*/bucket_[0-9]+", blob.name)
            if table_name:
                print("table_name", table_name)
                table_name = str(table_name[0])
                uri = "{}/{}".format(to_path, blob.name)

                load_job = client.load_table_from_uri(
                    uri, table_id1, job_config=job_config
                )
                load_job.result()
    print(
        "=====================GCS TO BQ (SRUCT)======================================"
    )
    print(f"Added {to_path} to GCS")

    # to get the right format
    table_idl = os.environ.get("table_idl")
    table_idl = table_idl + element

    tablename_extra = table_idl + "_extra"
    job_config = bigquery.QueryJobConfig(destination=table_idl)

    sql = """
        select row.* from {};
        """.format(
        tablename_extra
    )
    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.

    # Delete the extra table

    print("Query results loaded to the table {}".format(table_idl))
    client.delete_table(tablename_extra, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(tablename_extra))


def delete_tables(element):
    table_id = os.environ.get("table_id")
    table_id = table_id + element
    dataset_id = os.environ.get("dataset_id")
    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    # open('Source_DDL/source_ddl.sql', 'w').close()

    with open("config.json") as config_file:
        data = json.load(config_file)
        conn = hive.Connection(
            host=data["details"]["server"],
            username=data["details"]["user"],
            port=data["details"]["port"],
            database=data["details"]["database"],
        )
    cursor = conn.cursor()

    ddl = []
    complete_ddl = []

    tb_query = "show tables"
    # cursor = conn.cursor()
    cursor.execute(tb_query)
    table_id = os.environ.get("table_id")
    dataset_id = os.environ.get("dataset_id")
    lshive = []
    get_table = cursor.fetchall()
    df_table = pd.DataFrame(get_table)
    print(df_table)
    for row in df_table.index:
        lshive.append(df_table[0][row])
        data_query = "describe {}".format(df_table[0][row])
        cursor.execute(data_query)
        res = cursor.fetchall()
        dataframe_describe_column = pd.DataFrame(res)
        print(dataframe_describe_column)

    client = bigquery.Client()

    query = """        SELECT table_name FROM {} INFORMATION_SCHEMA.TABLES;
    """.format(
        dataset_id
    )
    results = client.query(query)
    for row in results.result():
        print("The tables are:", row)
    lsbq = []

    for row in results.result():
        lsbq.append(row[0])

    print("The hive tables are: ")
    print(lshive)
    # delete_tables()

    for element in lsbq:
        if element not in lshive:
            print("deleting table: ", element)
            # delete_tables(element)

    # To create tables
    elements = []
    for element in lshive:
        if element not in lsbq:
            print("Creating table: ", element)
            elements.append(element)

    print(elements)

    pool_size = 5  # your "parallelness"

# define worker function before a Pool is instantiated

pool = Pool(pool_size)

for element in elements:
    pool.apply_async(worker, (element,))

pool.close()
pool.join()
end = timer()
print("Time taken to run the code: ", end - start)
