# Fake News Classification Project

A distributed system for collecting, processing, and classifying news articles as real or fake using machine learning techniques.

## Project Overview

This project implements an end-to-end pipeline for fake news detection using a distributed architecture built on cloud services. We collect news data from multiple sources, store it in a scalable database, process it using distributed computing, and apply machine learning to classify articles as real or fake.

## Technologies Used

- **Data Collection**:
  - Google News API
  - Twitter API (limited due to API restrictions)
  - Kaggle datasets (for training data)
  - Apache Airflow for workflow orchestration

- **Storage**:
  - Google Cloud Storage (GCS) buckets
  - MongoDB Atlas

- **Processing**:
  - Apache Spark for distributed processing
  - PySpark for data preprocessing and feature extraction

- **Classification**:
  - Logistic Regression model
  - Feature extraction techniques

## Repository Contents

### Airflow DAG
The `airflow_dag` directory contains the Directed Acyclic Graph (DAG) configuration and related files for automating data collection from Google News API every two days.

- `fetch_upload_gcc.py`: Main DAG definition for fetching and uploading news data to Google Cloud Composer
- `dag_data.png`: Visual representation of the DAG data flow and structure
- `dag_run.png`: Screenshot of successful DAG execution and workflow

### PySpark SQL Aggregations
The `pyspark_sql_aggregations` directory contains scripts for data preprocessing, feature extraction, and aggregation analysis using Apache Spark.

- `spark_sql_agg.ipynb`: Jupyter notebook with data cleaning, feature extraction, and statistical analysis using PySpark SQL

### MongoDB
The `mongodb` directory contains database schemas, connection utilities, and example queries for working with the MongoDB Atlas database.

- `mongodb_ml_model/`: Directory containing ML model implementation for fake news classification using MongoDB data
- `mongodb_queries/`: Collection of MongoDB queries for data retrieval, updating, and analysis

## Project Journey

### Phase 1: Planning and Initial Setup
- Created project timeline and goals
- Evaluated multiple datasets and ideas
- Selected fake vs. real news classification as the project focus
- Set up Google Cloud Storage bucket
- Developed data acquisition strategies using Kaggle datasets for training

### Phase 2: Data Collection
- Attempted Twitter API integration (limited by API restrictions)
- Implemented Google News API for bulk article retrieval
- Developed and deployed Airflow DAG on Google Cloud Composer
- Scheduled periodic news article collection (every two days)
- Transferred data from GCS bucket to MongoDB Atlas for unified access

### Phase 3: Processing and Classification
- Implemented data processing pipeline with PySpark
- Trained a Logistic Regression model on 10,000 randomly selected training samples
- Applied the model to classify news articles as real or fake
- Stored classification results back in MongoDB Atlas

## Setup and Installation

### Prerequisites
- Google Cloud Platform account
- MongoDB Atlas account
- Python 3.7+
- Apache Airflow
- Apache Spark
- Required Python packages (see `requirements.txt`)

### Configuration
1. Create a Google Cloud Storage bucket
2. Set up a MongoDB Atlas cluster
3. Configure connection strings in the respective configuration files
4. Set up Google Cloud Composer environment
5. Deploy the Airflow DAG

### Running the Pipeline
1. The Airflow DAG will automatically run on the specified schedule
2. Alternatively, trigger the DAG manually through the Airflow UI
3. Run the PySpark scripts for data processing and model training
4. Query MongoDB Atlas for results and analysis

## Future Improvements
- Improve the classification model with more advanced techniques
- Expand data sources for more comprehensive coverage
- Implement real-time classification of news articles
- Develop a user interface for querying and visualizing results

## Contributors
- Sehej Singh
- Sang Ahn
- Samanvitha Bayaneni
- Arjun Bedi
- Hadley Dixon
