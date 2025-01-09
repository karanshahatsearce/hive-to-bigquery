# Hive-to-bigquery
sudo apt-get update
sudo apt-get install python3-pip
wget -P /tmp https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh
sh /tmp/Anaconda3-2019.10-Linux-x86_64.sh
source ~/.bashrc
conda activate

#Follow this steps:-
gcloud dataproc clusters create demo-hive-to-b2 \
--enable-component-gateway \
--region us-central1  \
--zone us-central1-f  \
--master-machine-type n2-standard-4 \
--master-boot-disk-size 500 \
--num-workers 2 \
--worker-machine-type n2-standard-4 \
--worker-boot-disk-size 500 \
--image-version 2.0-debian10 \
--project applied-ai-practice00 \
--service-account=test-demo@applied-ai-practice00.iam.gserviceaccount.com

gcloud compute ssh demo-hive-to-b2-m --project=applied-ai-practice00 --zone=us-central1-f
sudo su
cd /opt
git clone https://github.com/Pradnya-Koli/Hive-to-bigquery.git
bash prerequisite-installation.sh
bash auto-conf-setup-incremental.sh
createinsert.sql



#step2 
bash start.sh
