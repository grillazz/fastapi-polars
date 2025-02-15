# fastapi-polars

## Problem to Solve

Parquet is a highly useful and popular format for storing data in one place. However, it has one key limitation: extending an existing Parquet file is not straightforward.

Additionally, Parquet files are often stored on S3, which is not highly responsive and was not designed for low-latency operations.

## Solution

The FastAPI-Polars project aims to provide a fast, simple, and cost-effective solution to gather and store large volumes of data. The key features include:

- **Ursa Framework**: A big data framework to ingest, validate, and harmonize data using REST as the input protocol.
- **Polars DataFrame**: Utilized as an in-memory sink, delivering low-latency data storage based on Parquet files, which are then transported to S3 as a persistence layer in scheduled or ad-hoc steps.
- **Simplicity and Low Cost**: The main goal is to achieve simplicity and low cost of usage.

On S3, the project uses merge operations to combine files for the same DataFrame for the same day from different Kubernetes nodes with a gateway. Alternatively, it can open them in one Polars operation, sort, and write to a new Parquet file.

### FastAPI-Polars Project

#### 1. Architecture
The FastAPI-Polars project is designed with a microservices architecture, leveraging FastAPI for building the API endpoints and Polars for efficient data manipulation. The architecture consists of the following key components:
- **API Layer**: Built using FastAPI, this layer handles HTTP requests and routes them to the appropriate service functions.
- **Service Layer**: Contains business logic and interacts with the data layer. Services are implemented as classes, such as `S3Service`, which handles interactions with S3 storage.
- **Data Layer**: Utilizes Polars for data processing and manipulation, providing high performance for large datasets.
- **Storage Layer**: Uses MinIO, an S3-compatible object storage, for storing Parquet files.

#### 2. Used Python Libraries
- **FastAPI**: A modern, fast (high-performance) web framework for building APIs with Python 3.7+ based on standard Python type hints. It is chosen for its speed, ease of use, and automatic generation of OpenAPI documentation.
- **Polars**: A DataFrame library designed for high-performance data manipulation. It is chosen for its speed and efficiency, especially with large datasets, outperforming traditional libraries like Pandas.
- **s3fs**: A Pythonic file interface to S3, allowing for easy interaction with S3-compatible storage like MinIO. It is chosen for its simplicity and integration with other Python libraries.
- **attrs**: A library for creating classes with less boilerplate code. It is used to define service classes with attributes and automatic initialization.
- **MinIO**: An S3-compatible object storage server. It is chosen for its simplicity, scalability, and compatibility with the S3 API, making it easy to integrate with existing tools and libraries.

#### 3. Possible Usage
The FastAPI-Polars project can be used in various scenarios, including:
- **Data Ingestion**: Collecting and storing large volumes of data in Parquet format for efficient storage and retrieval.
- **Data Processing**: Performing complex data transformations and aggregations using Polars, which is optimized for performance.
- **Data Analytics**: Providing APIs for querying and analyzing data stored in S3, enabling data scientists and analysts to access and manipulate data efficiently.
- **ETL Pipelines**: Building Extract, Transform, Load (ETL) pipelines to process and move data between different systems and storage solutions.

#### 4. Problems and Gaps Solved
The FastAPI-Polars project addresses several challenges:
- **Performance**: By using Polars, the project can handle large datasets efficiently, providing faster data processing compared to traditional libraries like Pandas.
- **Scalability**: The microservices architecture allows for easy scaling of individual components, ensuring the system can handle increased load and data volume.
- **Ease of Use**: FastAPI's automatic documentation and type hinting make it easy for developers to build and maintain APIs, reducing development time and effort.
- **Storage Efficiency**: Using Parquet format for data storage ensures efficient use of storage space and faster read/write operations, especially for large datasets.
- **Compatibility**: The use of MinIO and s3fs ensures compatibility with the S3 API, allowing for seamless integration with other tools and services that support S3 storage.

Sure, here is a step-by-step guide to set up the FastAPI-Polars project:

### 1. Prerequisites
Ensure you have the following installed for local development:
- Python 3.13+
- Docker
- Docker Compose

### 2. Clone the Repository
Clone the project repository from GitHub:
```bash
git clone https://github.com/your-username/fastapi-polars.git
cd fastapi-polars
```

### 3. Set Up Environment Variables
Create a `.env` file in the project root directory and add the necessary environment variables:
```env
S3_KEY=your_s3_key
S3_SECRET=your_s3_secret
S3_ENDPOINT_URL=http://localhost:9000
```

### 4. Run local uvicorn with uv
```shell
(fastapi-polars) mac@mac fastapi-polars % uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload --loop uvloop --http httptools --log-level debug
INFO:     Will watch for changes in these directories: ['/Users/waco/PycharmProjects/fastapi-polars']
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [2749] using WatchFiles
INFO:     Started server process [2751]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     127.0.0.1:51509 - "GET / HTTP/1.1" 200 OK
INFO:     127.0.0.1:51509 - "GET /favicon.ico HTTP/1.1" 404 Not Found
INFO:     127.0.0.1:51512 - "GET /docs HTTP/1.1" 200 OK
INFO:     127.0.0.1:51512 - "GET /openapi.json HTTP/1.1" 200 OK
```

### 4. Run S3 on MinIO with Docker Compose
The `compose-s3.yaml` file is already configured to set up MinIO and create the necessary buckets. Run the following command to build and start the Docker containers:
```bash
(fastapi-polars) mac@mac fastapi-polars % docker compose -f compose-s3.yaml up -d
[+] Running 3/3
 ✔ Network parquet_content_pump_default            Created                                                                                                                                                                                    0.0s 
 ✔ Container minio                                 Started                                                                                                                                                                                    0.3s 
 ✔ Container parquet_content_pump-createbuckets-1  Started        
```

### 8. Access the API
Open your browser and navigate to `http://localhost:8000/docs` to access the automatically generated API documentation.

### Summary
You have now set up the FastAPI-Polars project with MinIO for S3-compatible storage. The project is ready for data ingestion, processing, and analytics using FastAPI and Polars.

## Query DataFrame
TODO: Query the DataFrame attached to the request via Polars live using Pyodide or Jupyter with WebSockets from the front end. Alternatively, access the Python kernel with a kernel gateway like in the Guardian project.

