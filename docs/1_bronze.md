## Bronze Layer

### `ingest_parquet_data_from_raw_file`

**What:**
Ingests data from raw CSV or JSON files and writes it to a Parquet file.

**Why:**
The Bronze Layer is responsible for bringing raw data into the pipeline in a structured format, making it ready for further processing and analysis.

**How:**
- Takes the raw data file path, output file path, file type (CSV or JSON), and other parameters.
- Uses PySpark to read the raw data and write it into Parquet format.
- Logs the process and handles exceptions to ensure robust data ingestion.
