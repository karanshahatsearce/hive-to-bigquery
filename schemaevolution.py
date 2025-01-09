import time
from google.cloud import bigquery
import csv
import os
import pandas as pd
import re
import pandas as pd
import findspark
import logging

findspark.init("/usr/lib/spark")
from pyhive import hive
from google.cloud import bigquery
import json

client = bigquery.Client()
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_NAME = os.environ.get("DATASET_NAME")
dataframe = []


def Alter_add_columns():
    with open("query_hive.csv") as file_obj:
        reader_obj = csv.reader(file_obj)
        dataset_id = os.environ.get("dataset_id")
        for row in reader_obj:
            print(row)
            text = str(row)
            format_query = (
                text.replace(".", " ")
                .replace("(", " ")
                .replace(")", " ")
                .replace("[", " ")
                .replace("]", " ")
                .replace(";", " ")
                .replace("'", " ")
                .replace(",", "")
            )
            #       print(format_query)
            phrase_to_list = format_query.split()
            #       print(phrase_to_list)
            length_list = len(phrase_to_list)
            #      print(length_list)
            dataframe = pd.DataFrame(phrase_to_list)
            print(dataframe)
            for i in phrase_to_list:
                #            print("-------------checking for alter add columns------------------------------>>>>>>>>>>>>")
                store_value = "ALTER"
                store_index = 3
                if i == "ALTER":

                    for row in dataframe.index:
                        if row == 3:
                            table_id = (
                                "applied-ai-practice00.hive_to_bigquery.{}".format(
                                    dataframe[0][row]
                                )
                            )
                            table = client.get_table(table_id)
                            print(table_id)
                            msg = "------------------------> set the bigquery destination table <----------------------------------------"

                            print("\033[92m {}\033[00m".format(msg))
                            # print("--------------------------set the bigquery destination table----------------------->")
                            for row in dataframe.index:
                                if row == 6:
                                    while row != length_list:
                                        msg1 = "<-------------------------------fetching the add columns  --------------------------------------->"
                                        print("\033[92m {}\033[00m".format(msg1))
                                        value = dataframe[0][row]
                                        data_type = dataframe[0][row + 1]
                                        print(row)
                                        row += 2

                                        #                        print("_increment_continue_row",continue_row)
                                        original_schema = table.schema
                                        new_schema = original_schema[:]

                                        new_schema.append(
                                            bigquery.SchemaField(value, data_type)
                                        )

                                        #                     print(new_schema)
                                        table.schema = new_schema
                                        table = client.update_table(
                                            table, ["schema"]
                                        )  # Make an API request.

                                        if (
                                            len(table.schema)
                                            == len(original_schema) + 1
                                            == len(new_schema)
                                        ):
                                            msg2 = "A new column has been added."
                                            print("\033[92m {}\033[00m".format(msg2))

                                        else:
                                            msg3 = "The column has not been added."
                                            print("\033[92m {}\033[00m".format(msg3))


def Alter_Rename_column_name():
    with open("query_hive.csv") as file_obj:
        dataset_id = os.environ.get("dataset")
        print(dataset_id)
        reader_obj = csv.reader(file_obj)
        for row in reader_obj:
            text = str(row)
            format_query = (
                text.replace(".", " ")
                .replace("(", " ")
                .replace(")", " ")
                .replace("[", " ")
                .replace("]", " ")
                .replace(";", " ")
                .replace("'", " ")
                .replace(",", "")
            )
            #       print(format_query)
            phrase_to_list = format_query.split()
            #       print(phrase_to_list)
            length_list = len(phrase_to_list)
            #      print(length_list)
            dataframe = pd.DataFrame(phrase_to_list)
            print(dataframe)
            for row in dataframe.index:
                if row == 3:
                    table_id = "applied-ai-practice00.hive_to_bigquery.{}".format(
                        dataframe[0][row]
                    )
                    print(table_id, "---------------------------")
                    table = client.get_table(table_id)
                if row == 5:
                    query_rename = ("ALTER TABLE {} RENAME COLUMN {} TO {};").format(
                        table_id, dataframe[0][row], dataframe[0][row + 1]
                    )
                    query_job = client.query(query_rename)

                #  print("Waiting for job to finish")
                #  query_job.reload()
                #  print(query_job.result())


def Drop_column():
    with open("query_hive.csv") as file_obj:
        reader_obj = csv.reader(file_obj)
        dataset_id = os.environ.get("dataset_id")
        for row in reader_obj:
            text = str(row)
            format_query = (
                text.replace(".", " ")
                .replace("(", " ")
                .replace(")", " ")
                .replace("[", " ")
                .replace("]", " ")
                .replace(";", " ")
                .replace("'", " ")
                .replace(",", "")
            )
            #       print(format_query)
            phrase_to_list = format_query.split()
            #       print(phrase_to_list)
            length_list = len(phrase_to_list)
            #      print(length_list)
            dataframe = pd.DataFrame(phrase_to_list)
            print(dataframe)
            for row in dataframe.index:
                if row == 2:
                    table_id = "{}{}".format(dataset_id, dataframe[0][row])
                    table = client.get_table(table_id)
                if row == 5:
                    query = ("""ALTER TABLE `{}` DROP COLUMN IF EXISTS {};""").format(
                        table_id, dataframe[0][row]
                    )
                    print(query)
                    query_job = client.query(query)


#                    while query_job.state != 'Done':
#                       print("Waiting for job to finish")
#                      query_job.reload()
#                     print(query_job.result())
