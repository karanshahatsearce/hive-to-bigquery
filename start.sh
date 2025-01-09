# script1.sh
#!/bin/bash
BUCKET_NAME="demo-gcs-bq-test"

# Check if the bucket exists
if gsutil ls "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "Bucket already exists: ${BUCKET_NAME}"
else
  echo "Bucket does not exist: ${BUCKET_NAME}"
  # Create the bucket
  gsutil mb "gs://${BUCKET_NAME}"
  echo "Bucket created: ${BUCKET_NAME}"
fi

PROJECT_ID="applied-ai-practice00"
DATASET_NAME="hive_to_bigquery"

# Check if the dataset exists
if bq show "${PROJECT_ID}:${DATASET_NAME}" >/dev/null 2>&1; then
  echo "Dataset already exists: ${DATASET_NAME}"
else
  echo "Dataset does not exist: ${DATASET_NAME}"
  # Create the dataset
  bq mk --project_id "${PROJECT_ID}" "${DATASET_NAME}"
  echo "Dataset created: ${DATASET_NAME}"
fi

# Set another variable
export from_path="hdfs://demo-hive-to-b2-m/user/hive/warehouse/hivedemo.db/"
echo "from_path ==>",$from_path
export to_path="gs://${BUCKET_NAME}"
echo "to_path ==>",$to_path
export project="applied-ai-practice00"
echo "==> project-Id",$project
export bucket_name={BUCKET_NAME}
echo "==>bucket_name",$bucket_name
export table_id="applied-ai-practice00.hive_to_bigquery."
echo "==>table_id",$table_id
export dataset_id="applied-ai-practice00.hive_to_bigquery."
echo "==>dataset_id",$dataset_id
export table_idl="applied-ai-practice00.hive_to_bigquery."

# Prompt the user to select a script
echo "Please select a script to execute:"
echo "1. Historic-load"
echo "2. Incremental-load"
echo "3. Schema evolution"
read -p "Enter the script number: " script_number

# Execute the selected script

case "$script_number" in
  "1")
    echo "Executing Historic Migration script..........."
    python Batch-historic-load.py
    ;;
  "2")
    echo "Executing Incremental Migration script........"
    cd Incremental
    sbt run
    ;;
  "3")
    echo "Executing Schema Evolution Migration script......"
    echo "Please select a schema evolution:"
    echo "1. Add_columns"  
    echo "2. Rename columns" 
    echo "3. Drop columns"
    read -p "select the change of schema: " number
    case "$number" in 
    "1")
      echo "executing the add column function..........."
      python main.py --source ADD_COLUMNS
      ;;
    "2")
      echo "executing the rename column function..........."  
      python main.py --source RENAME_COLUMN
      ;;
    "3")
      echo "executing the drop column function........"
      python main.py --source DROP_COLUMN
      ;;
    *)
    echo "Invalid number entered."
    ;;
    esac
#  *)
  echo "Invalid number entered."
  ;;
esac
