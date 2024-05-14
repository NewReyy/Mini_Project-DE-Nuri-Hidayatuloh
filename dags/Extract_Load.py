from datetime import timedelta
import pandas as pd
import sqlite3
import requests
import csv
import requests
from google.cloud import storage
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas_gbq
from google.oauth2 import service_account

def extract_data():
    try:
        urls = {
            "population": 'https://api.worldbank.org/v2/countries/all/indicators/SP.POP.TOTL/?format=json&per_page=30000',
            "gdp": 'https://api.worldbank.org/v2/countries/all/indicators/NY.GDP.MKTP.CD/?format=json&per_page=30000',
            "electricity": 'https://api.worldbank.org/v2/countries/all/indicators/EG.ELC.ACCS.ZS/?format=json&per_page=30000',
            "rural": 'https://api.worldbank.org/v2/countries/all/indicators/SP.RUR.TOTL.ZS/?format=json&per_page=30000'
        }

        data_frames = {
            "population": [],
            "gdp": [],
            "electricity": [],
            "rural": []
        }

        for key, url in urls.items():
            response = requests.get(url)
            data = response.json()[1]

            for item in data:
                country_code = item["countryiso3code"]
                country = item["country"]["value"]
                indicator = item["indicator"]["id"]
                indicator_name = item["indicator"]["value"]
                value = item["value"]
                date = item["date"]

                if key == "population":
                    data_frames[key].append({
                        "Country Code": country_code,
                        "Country Name": country,
                        "Indicator Code": indicator,
                        "Indicator Name": indicator_name,
                        "Value": value,
                        "Year": date,
                    })
                elif key == "gdp":
                    data_frames[key].append({
                        "Country Name": country,
                        "Country Code": country_code,
                        "Indicator Name": indicator_name,
                        "Indicator Code": indicator,
                        "Year": date,
                        "GDP": value,
                    })
                elif key == "electricity":
                    data_frames[key].append({
                        "Country Name": country,
                        "Country Code": country_code,
                        "Indicator Name": indicator_name,
                        "Indicator Code": indicator,
                        "Year": date,
                        "electricityaccesspercent": value,
                    })
                elif key == "rural":
                    data_frames[key].append({
                        "Country Name": country,
                        "Country Code": country_code,
                        "Indicator Name": indicator_name,
                        "Indicator Code": indicator,
                        "Year": date,
                        "ruralpopulationpercent": value,
                    })

        for key, data_list in data_frames.items():
            df = pd.DataFrame(data_list)
            df.to_csv(f"data_extract/{key}.csv", index=False)

    except Exception as e:
        print(f"Error occurred during data extraction: {e}")
        raise

def load_csv_to_sqlite(csv_file, db_file):
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS data (
                        id INTEGER PRIMARY KEY,
                        country_name TEXT,
                        country_code TEXT,
                        year INTEGER,
                        population INTEGER,
                        gdp REAL,
                        gdp_per_capita REAL,
                        rural_population_percent REAL,
                        electricity_access_percent REAL,
                        project_cost REAL
                    )''')

        with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                c.execute('''INSERT INTO data (
                                country_name,
                                country_code,
                                year,
                                population,
                                gdp,
                                gdp_per_capita,
                                rural_population_percent,
                                electricity_access_percent,
                                project_cost
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (
                                row['countryname'],
                                row['countrycode'],
                                int(row['year']),
                                int(row['population']),
                                float(row['gdp']),
                                float(row['gdppercapita']),
                                float(row['ruralpopulationpercent']),
                                float(row['electricityaccesspercent']),
                                float(row['projectcost'])
                            ))

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error occurred during loading CSV to SQLite: {e}")
        raise

def load_csv_scaled_to_sqlite(csv_file, db_file):
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS data (
                        id INTEGER PRIMARY KEY,
                        country_name TEXT,
                        country_code TEXT,
                        year INTEGER,
                        population REAL,
                        gdp REAL,
                        gdp_per_capita REAL,
                        rural_population_percent REAL,
                        electricity_access_percent REAL,
                        project_cost REAL
                    )''')

        with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                c.execute('''INSERT INTO data (
                                country_name,
                                country_code,
                                year,
                                population,
                                gdp,
                                gdp_per_capita,
                                rural_population_percent,
                                electricity_access_percent,
                                project_cost
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (
                                row['countryname'],
                                row['countrycode'],
                                int(row['year']),
                                float(row['population']),
                                float(row['gdp']),
                                float(row['gdppercapita']),
                                float(row['ruralpopulationpercent']),
                                float(row['electricityaccesspercent']),
                                float(row['projectcost'])
                            ))

        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Error occurred during loading scaled CSV to SQLite: {e}")
        raise

def upload_files_to_gcs(service_account_json, bucket_name, file_paths):
    try:
        client = storage.Client.from_service_account_json(service_account_json)
        
        for local_path, gcs_filename in file_paths:
            df = pd.read_csv(local_path)
            
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(gcs_filename)
            blob.upload_from_filename(local_path)
            
            print(f"Successfully uploaded {gcs_filename} to bucket: {bucket_name}.")

    except Exception as e:
        print(f"Error occurred during file upload to GCS: {e}")
        raise

def load_to_bigquery():
    try:
        # Muat kredensial dari file token
        credentials = service_account.Credentials.from_service_account_file('newKeyService.json')
        df_to_gbq = pd.read_csv('data_final/data_final.csv')

        project_id = "mini-project-423106"
        table_id = 'mini_project.data_airflow'

        pandas_gbq.to_gbq(df_to_gbq, table_id, project_id=project_id, credentials=credentials)

    except Exception as e:
        print(f"Error occurred during loading to BigQuery: {e}")
        raise

default_args = {
    'owner': 'NURI',
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Extract_Load_Data',
    default_args=default_args,
    schedule_interval='@weekly',
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    load_csv_task = PythonOperator(
        task_id='load_csv_to_sqlite',
        python_callable=load_csv_to_sqlite,
        op_args=['data_final/data_final.csv', 'data_final/data_final.db'],
        dag=dag,
    )

    load_csv_scaled_task = PythonOperator(
        task_id='load_csv_scaled_to_sqlite',
        python_callable=load_csv_scaled_to_sqlite,
        op_args=['data_final/data_final_scaled.csv', 'data_final/data_final_scaled.db'],
        dag=dag,
    )

    upload_gcs_task = PythonOperator(
        task_id='upload_files_to_gcs',
        python_callable=upload_files_to_gcs,
        op_args=['serviceAccount.json', 'mini-project-data-engineer-bucket-nuri',
                 [
                     ("data_final/data_final.csv", "data_final.csv"),
                     ("data_final/data_final_scaled.csv", "data_final_scaled.csv")
                 ]],
        dag=dag,
    )

    load_bigquery_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        dag=dag,
    )

extract_task >> [load_csv_task, load_csv_scaled_task] >> upload_gcs_task >> load_bigquery_task