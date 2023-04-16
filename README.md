# Data Warehouse Design for E-commerce Environments

>## Table of Contents
>[About Files](#about-files) <br />
>[Problem Statement](#problem-statement) <br />
>[Dataset](#dataset) <br />
>[Technology Involved](#technology-involved) <br />
>[Final Analysis Report](#analysis-report) <br />

## About Files
**dagnew.py** - DAG File to Schedule to run Python File on a regular interval
**sales_data.csv** - Part of Source File
**test3.py** - Python File for ETL Process

## Problem Statement
You will be constructing a data warehouse for a retail e-commerce store in this project. You would also be expected to answer a few particular issues about pricing optimization and inventory allocation in terms of design and implementation. In this project, you'll be attempting to answer the following two questions:
-	Were the higher-priced items more prevalent in some markets?
-	Should inventory be reallocated or prices adjusted based on location?

## Dataset
I've taken a pretty random e-commerce dataset to challenge myself, whether or not, I'll be able to answer these questions
[E-Commerce Dataset Link](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data)

## Technology Involved
* Data Source Stored in **Google Cloud Storage Bucket**
![DAGs - ecommerce-env - Brave 16-04-2023 23_29_31](https://user-images.githubusercontent.com/78255808/232334610-cbbeb704-d1d3-42e4-a375-f4eda2edbd14.png)
__(Google Cloud Storage Screenshot)__
<br />

* Data Extraction using **Bigquery Python Package** to convert .csv file to Pandas Dataframe
![DAGs - ecommerce-env - Brave 16-04-2023 23_27_58](https://user-images.githubusercontent.com/78255808/232334731-4c5c5634-93b5-47e2-a843-cb62783b83d3.png)
__(Bigquery Screenshot)__
Here, 'Sales Report' is the source file, while other tables are the Processed & Transformed Data stored in Tables
<br />

* Data Transformation and Processing using **Pandas Python Package**
<br />

* Data Loading using same **Bigquery Python Package** to store Dataframe into Data Warehouse table
<br />

* **ETL Pipeline** is constructed using **Cloud Composer API** with **Apache Airflow** where this Python Scripts will be executed regularly in pre-defined time intervals
![DAGs - ecommerce-env - Brave 16-04-2023 23_15_31](https://user-images.githubusercontent.com/78255808/232334908-cde540d4-17d0-4571-afad-d8d0d9d51dd0.png)
![DAGs - ecommerce-env - Brave 16-04-2023 23_19_03](https://user-images.githubusercontent.com/78255808/232334917-7a3aca5b-a179-4a8c-9510-94498d5e33fa.png)
__(Cloud Composer & Airflow Screenshots)__
<br />

* Finally the data is then exported to **Google Looker Studio**
<br />

## Analysis Report
The final analysis is presented in a visually and data-driven manner in **Google Looker Studio**. [Click here to View it](https://lookerstudio.google.com/reporting/aa8d500e-b262-449d-9fba-e0d5b591d7d0)

