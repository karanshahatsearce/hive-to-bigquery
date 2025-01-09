import argparse
from  schemaevolution import *


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Hive Database for Schema Conversion
    """)
    parser.add_argument("--source", help="list of DML operation")

    args = parser.parse_args()

    Source = args.source

    if Source == "ADD_COLUMNS":
        Alter_add_columns()

        print("Schema Migrated Successfully")
    if Source == "RENAME_COLUMN":
        Alter_Rename_column_name()

        msg="Successfully Rename the table Name in Bigquery"
        print("\033[92m {}\033[00m" .format(msg))
    if Source == "DROP_COLUMN":
        Drop_column()

        msg="Successfully drop column from  Bigquery"
        print("\033[92m {}\033[00m" .format(msg))
