# Vehicle Accidents Pipeline - Cobli Case

This repository contains the code for an ETL pipeline developed in Apache Airflow to process vehicle accident data. The pipeline performs a series of transformations to extract, clean, transform, and enrich the data, preparing it for subsequent analyses.

## Pipeline Structure

The pipeline consists of several tasks executed sequentially. Each task represents a step in the ETL process. The DAG named `vehicle_accidents_cobli` orchestrates the execution of these tasks in Airflow.

## Requirements

To execute this pipeline, Apache Airflow must be configured and running. Additionally, project dependencies should be installed as described in the [Installation](#installation) section.

## Installation

1. Ensure Python version 3.12 is installed.
2. Install Apache Airflow by following the [official Docker documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).
3. Configure the file connection (`file_conn`) so the input file can be detected.
4. Clone this repository:
   ```bash
   git clone https://github.com/mateusfgomes/airflow-cobli.git

5. To create all necessary directories for correct execution, run the `docker compose build` command before using `docker compose up` to ensure the Dockerfile is executed beforehand.  
6. Execute the following command in a virtual environment:  
   ```bash
   pip install -r requirements.txt

### File Structure

- `dags/`: Contains the main DAG file.  
- `scripts/`: Contains Python scripts used in pipeline tasks.

## Dataset Exploration (Databricks - Extra)

Before implementing the required transformations and DAGs in Airflow, the dataset was explored using the Databricks platform. This exploration provided better insights into the data context, enabling a clearer understanding of data cleaning and structuring requirements. The Databricks notebook is available in this repository.

## Task Execution

![Pipeline Execution Flow](./logs_images/dag_ran.png)

1. **File Sensor (`task_file_sensor`)**  
   Monitors the presence of the `acidentes_brasil.csv` file. When the file is detected, the pipeline execution begins.  
   - **fs_conn_id**: File system connection identifier.  
   - **filepath**: Path to the input file.  
   - **poke_interval**: Time interval, in seconds, to check for file existence.

![Sensor](./logs_images/sensor.png)

2. **Load Data (`task_load_data`)**  
   Loads data from the CSV file (`acidentes_brasil.csv`) into the pipeline, converting it into a Spark DataFrame. The data is stored temporarily in `/tmp/bronze/car_accidents`, partitioned by year and state (UF).

![Table Loading](./logs_images/carregamento_tabela.png)  
![Year Column Creation](./logs_images/criando_coluna_ano.png)  
![Schema Loading](./logs_images/schema_carregado.png)

3. **Send to Bronze (`task_send_to_bronze`)**  
   Moves the loaded data to the Bronze layer, the first storage layer in the data pipeline. While this layer is implemented as a directory here, it could integrate with external systems like AWS S3 using an S3 Hook for more elaborate processing. Data is partitioned by year and UF.

![Sending to Bronze Layer](./logs_images/logs_send_bronze.png)

4. **Filter Null (`task_filter_null`)**  
   Filters records with null values in any column to ensure data integrity. Additionally, the `classificacao_acidente` column is processed to replace "NA" values with more appropriate classifications based on the number of fatalities and injuries. Filtered data is saved temporarily in `/tmp/silver/car_accidents_filtered` before moving to the Silver layer.

![Filtering Logs](./logs_images/logs_filtered_null.png)

5. **Casting Fields (`task_casting_fields`)**  
   Converts data types to ensure each field is in the correct format. For example:  
   - Fields such as `id`, `br`, and `km` are converted to numerical types.  
   - Date and time fields are adjusted to `DateType` and `TimestampType`.  
   - Latitude and longitude fields are corrected to replace commas with dots.  
   Processed data is temporarily stored in `/tmp/silver/car_accidents_casted`, partitioned by year.

![Field Casting Logs](./logs_images/logs_casting_fields.png)  
![Collapsed Casting Logs](./logs_images/logs_casting_fields_collapsed.png)

6. **Enriching Data (`task_enriching_data`)**  
   Enriches data by calculating new columns, such as `percentual_fatalidades`, which represents the percentage of fatalities relative to the total number of individuals involved in each accident. Enriched data is saved in `/tmp/silver/car_accidents_enriched`, compressed with Snappy, and partitioned by year.

![Table Enrichment](./logs_images/logs_nriching_data.png)

7. **Send to Silver (`task_send_to_silver`)**  
   Moves enriched data from the temporary folder to the Silver data lake directory, partitioned only by year, completing the pipeline flow.

![Sending to Silver Layer](./logs_images/logs_save_silver.png)
