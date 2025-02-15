# fastapi-polars

## Problems to Solve

Parquet is a highly useful and popular format for storing data in one place. However, it has one key limitation: extending an existing Parquet file is not straightforward.

Additionally, Parquet files are often stored on S3, which is not highly responsive and was not designed for low-latency operations.

## Solution

The FastAPI-Polars project aims to provide a fast, simple, and cost-effective solution to gather and store large volumes of data. The key features include:

- **Ursa Framework**: A big data framework to ingest, validate, and harmonize data using REST as the input protocol.
- **Polars DataFrame**: Utilized as an in-memory sink, delivering low-latency data storage based on Parquet files, which are then transported to S3 as a persistence layer in scheduled or ad-hoc steps.
- **Simplicity and Low Cost**: The main goal is to achieve simplicity and low cost of usage.

On S3, the project uses merge operations to combine files for the same DataFrame for the same day from different Kubernetes nodes with a gateway. Alternatively, it can open them in one Polars operation, sort, and write to a new Parquet file.

## Setup Instructions

1. **Setup Local Environment with Uvicorn**
   - [Link to Astral](#)

1a. **Setup with Uvicorn**
1b. **Setup with FastAPI**
1c. **Setup with Docker**
1d. **Setup with Kubernetes**

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
Ensure you have the following installed:
- Python 3.7+
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

### 4. Set Up Docker Compose
The `docker-compose.yaml` file is already configured to set up MinIO and create the necessary buckets. Ensure it looks like this:
```yaml
name: parquet_content_pump
services:
  minio:
    container_name: minio
    image: minio/minio:latest
    ports:
      - 9000:9000
      - 9001:9001
    volumes:
      - datastore:/data
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
    command: server /data --console-address ":9001"

  createbuckets:
    image: minio/mc
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      /usr/bin/mc alias set myminio http://minio:9000 minio minio123;
      /usr/bin/mc mb myminio/daily;
      /usr/bin/mc anonymous set public myminio/daily;
      exit 0;
      "
volumes:
  datastore:
```

### 5. Build and Run Docker Containers
Run the following command to build and start the Docker containers:
```bash
docker-compose up -d
```

### 6. Install Python Dependencies
Create a virtual environment and install the required Python packages:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
pip install -r requirements.txt
```

### 7. Run the FastAPI Application
Start the FastAPI application:
```bash
uvicorn main:app --reload
```

### 8. Access the API
Open your browser and navigate to `http://localhost:8000/docs` to access the automatically generated API documentation.

### Summary
You have now set up the FastAPI-Polars project with MinIO for S3-compatible storage. The project is ready for data ingestion, processing, and analytics using FastAPI and Polars.

## Query DataFrame

TODO: Query the DataFrame attached to the request via Polars live using Pyodide or Jupyter with WebSockets from the front end. Alternatively, access the Python kernel with a kernel gateway like in the Guardian project.

