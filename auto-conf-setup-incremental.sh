#!/bin/bash

# Define the properties to be added
#!/bin/bash

# Declare an associative array of properties to add
declare -A properties=(
    ["hive.support.concurrency"]="true"
    ["hive.txn.manager"]="org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"
    ["hive.enforce.bucketing"]="true"
    ["hive.exec.dynamic.partition.mode"]="nostrict"
    ["hive.compactor.worker.threads"]="5"
)

# Path to hive-site.xml
hive_site="/usr/lib/hive/conf/hive-site.xml"

# Backup the original hive-site.xml
cp "$hive_site" "${hive_site}.bak"

# Add properties to hive-site.xml if they don't exist already
for key in "${!properties[@]}"; do
    value="${properties[$key]}"
    # Check if the property already exists in hive-site.xml
    if ! grep -q "<name>${key}</name>" "$hive_site"; then
        # Create an XML element for the property
        xml="<property><name>${key}</name><value>${value}</value></property>"
        # Insert the XML element before the closing </configuration> tag in hive-site.xml
        sed -i "/<\/configuration>/i \\\t${xml}" "$hive_site"
        echo "Property ${key} added to $hive_site"
    else
        echo "Property ${key} already exists in $hive_site"
    fi
done

cd /usr/lib/hive/conf
cp hive-site.xml  /usr/lib/spark/conf


# Define properties to be added or modified in spark-conf
declare -A properties=(
  ["spark.sql.extensions"]="com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension"
  ["spark.kryo.registrator"]="com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator"
  ["spark.sql.hive.hwc.execution.mode"]="client"
  ["spark.hadoop.hive.metastore.uris"]="thrift://demo-hive-to-b1-m:9083"
)

# Iterate over properties and add or modify them in spark-defaults.conf
for property_name in "${!properties[@]}"; do
  property_value="${properties[$property_name]}"
  
  # Check if property already exists in spark-defaults.conf
  if grep -q "^$property_name=" /usr/lib/spark/conf/spark-defaults.conf; then
    # Modify existing property
    sed -i "s/^$property_name=.*/$property_name=$property_value/" /usr/lib/spark/conf/spark-defaults.conf
  else
    # Add new property at the end of file
    echo "$property_name=$property_value" >> /usr/lib/spark/conf/spark-defaults.conf
  fi
done
