# data-engineering-portifolio

## Data Pipeline with Apache Beam

**Key-words:** Apache Beam, Python, Pandas, regex, Batch processing, ETL.

A batch data pipeline, developed in Python using Apache Beam, that integrates two independent datasets (dengue cases and rainfall volume) into a single consolidated dataset organized by state and month.

- Ingestion and parsing: reading raw files (pipe-delimited TXT and CSV) and converting each line into a dictionary using column names.
- Dengue pipeline: creating a `year_month` key from the epidemiological week's start date, grouping by state (UF) using `GroupByKey`, and summing cases by state + `year_month` (using `FlatMap` and `CombinePerKey`).
- Rainfall pipeline: constructing a state + `year_month` key, handling invalid values ​​(negative numbers and failed measurements, such as -9999, are discarded), and summing monthly precipitation, rounded to one decimal place.
- Dataset integration: joining the two `PCollections` by key using `CoGroupByKey`, while removing records missing either rainfall or dengue data.
- Loading: exporting the result to CSV (`uf; year; month; precipitation_mm; dengue_cases`) and validating the output using pandas.

[Click here to access the repository](https://github.com/brnocesar/learning-data-analysis/tree/main/apache_beam)

## ELT Data Pipeline with Apache Airflow and Apache Spark

**Key-words:** Apache Airflow, Apache Spark, Python, Data Lake (JSON), ELT, Batch processing.

A batch data pipeline that extracts data from the Twitter API, stores the raw content in a data lake, and transforms it into a structured format ready for analysis.

- Daily extraction of Twitter data and ingestion of raw data into the data lake in JSON format, following the ELT approach (data is loaded first and transformed later).
- Data transformation using Apache Spark.
- Orchestration of the entire workflow using Apache Airflow: defining DAGs, scheduling daily execution, managing task dependencies, and monitoring each run.

[Click here to access the repository](https://github.com/brnocesar/airflow-spark)

## Analysis of Registration Data with Apache Spark

**Key-words:** Apache Spark, PySpark, Python.

A data processing project using PySpark aimed at exploring, cleaning, and analyzing registration data for Brazilian companies, establishments, and partners, sourced from partitioned CSV files from the CNPJ database.

- Ingestion: Reading partitioned CSV files into Spark DataFrames, with schema inference for the company, establishment, and partner datasets.
- Transformations: Column renaming, data type conversion, handling of monetary values ​​and dates, and field preparation for subsequent analysis.
- Analysis/Querying: Using the PySpark DataFrame API for record inspection, row counting, and initial data exploration.
- Output: Generation of processed DataFrames and the option to save them to local files in CSV or Parquet format for future use.

[Click here to access the repository](https://github.com/brnocesar/learning-data-analysis/tree/main/apache_spark)

## File and Stream Handling in PHP

**Key-words:** PHP, Streams, SPL.

A PHP project exploring the reading, writing, and processing of files in various formats, as well as the use of streams and wrappers to integrate the console, compressed files, directories, and HTTP requests.

- Text files: reading (full or line-by-line) and writing with incremental content updates.
- CSV: export and reading of structured data using an object-oriented approach.
- Streams: console input/output, direct access to ZIP files (including password-protected ones), and consumption of content from external HTTP endpoints, with stream contexts configured for each scenario.
- Persistence and transformation: copying content between streams, directory listing, and writing user-provided data to files.

[Click here to access the repository](https://github.com/brnocesar/learning-PHP/tree/main/11-input-output-streans)
