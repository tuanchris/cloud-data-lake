# cloud-data-lake
Comparison of data lake deployment on two cloud platforms: AWS and GCP

Create a project on GCP
Enable billing
Navigate to IAM and create a service account
Grant the account project owner (not recommended for production system)
Download the JSON credentials



```
git clone https://github.com/tuanchris/cloud-data-lake
conda create --name cloud-data-lake python=3.7
conda activate cloud-data-lake
cd cloud-data-lake
pip install --user -r requirements.txt
```

If airflow is not found, run the following commands:
```
sudo pip install --ignore-installed apache-airflow
```
To start airflow, run the following command:
```
/bin/bash start.sh
```
