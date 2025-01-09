echo "****** ****** Starting the setup for hive to bigquery ******* ********"
sudo apt-get update
sudo apt-get install python3-pip
wget -P /tmp https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh
sh /tmp/Anaconda3-2019.10-Linux-x86_64.sh
source ~/.bashrc
conda activate
echo "****** ****** installing pandas******* ********"
pip3 install findspark
pip3 install pyhive
pip install thrift
pip install pyhive
pip install findspark
pip install google
pip install google-api-python-client
pip install google-cloud-bigquery
pip install google-cloud-storage
sudo apt-get install libsasl2-dev python-dev libldap2-dev libssl-dev
pip install sasl
pip install thrift_sasl
echo "  setup the Hive acid transaction configuration file "
bash auto-conf-setup-incremental.sh

echo "  Restart the hive server "
sudo systemctl restart hive-server2

echo "successfully completed the setup for hive to bigquery migration run the next file start.sh "

hive -f createinsert.sql

wget https://repository.cloudera.com/artifactory/libs-release-local/com/hortonworks/hive/hive-warehouse-connector-spark3_2.12/1.0.0.1.18.7215.0-43/hive-warehouse-connector-spark3_2.12-1.0.0.1.18.7215.0-43.jar
wget https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
cd Incremental
sudo apt-get install sbt
sbt
