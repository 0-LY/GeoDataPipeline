# GeoDataPipeline

### Overview

**GeoDataPipeline** is a data engineering project designed to automate the collection, processing, and visualization of geospatial data through a microservices architecture. The project consists of the following key components:

* **ETL Service** : Extracts, transforms, and loads geospatial data into a PostgreSQL database with PostGIS.
* **API Service** : Exposes RESTful endpoints using **FastAPI** for querying and interacting with the geospatial data.
* **Database** : PostgreSQL with **PostGIS** extension for storing and managing geospatial data.
* **Jupyter Notebooks** : Interactive environment for data analysis and visualization.

### Technologies Used

* **FastAPI** : For building API endpoints.
* **PostgreSQL** with  **PostGIS** : For spatial data storage and querying.
* **Docker** &  **Docker Compose** : For managing services and containerizing the application.
* **Jupyter Notebook** : For analyzing and visualizing data.
* **AWS S3** : For storing and retrieving geospatial data.
* **GeoJSON** : A format for representing geospatial data.

### Features

* **ETL Service** : Collects raw data from external sources, processes it, and loads it into PostgreSQL with PostGIS.
* **Geospatial API** : Provides endpoints to query spatial data, such as events, users, and locations.
* **Data Visualization** : Interactive notebooks for exploring and visualizing geospatial data.

### Project Structure

```
.
├── docker-compose.yaml        # Docker Compose configuration for managing services
├── etl                       # ETL service to collect, process, and load data
│   ├── Dockerfile
│   ├── requirements.txt       # Python dependencies for the ETL service
│   └── src                   # Source code for the ETL service
├── fastapi                   # FastAPI service for API endpoints
│   ├── Dockerfile
│   ├── requirements.txt       # Python dependencies for FastAPI service
│   └── src                   # Source code for the FastAPI service
├── jupyter                   # Jupyter Notebook for data visualization
│   ├── Dockerfile
│   ├── notebooks              # Jupyter notebooks for analysis
│   └── requirements.txt       # Python dependencies for Jupyter service
└── postgres                  # PostgreSQL service with PostGIS extension
    ├── Dockerfile
    └── init
        └── init.sql           # SQL script to initialize the database schema
```

### Prerequisites

* **Docker** : To containerize the services.
* **Docker Compose** : To run multi-container applications.
* **Git** : For version control and cloning the repository.

### Setup and Installation

#### Step 1: Clone the Repository

Clone the repository to your local machine using the following command:

```bash
git clone https://github.com/0-LY/GeoDataPipeline.git
cd GeoDataPipeline
```

#### Step 2: Set Up Environment Variables

Create a `.env` file in the root directory of the project and define the following environment variables.  **Make sure to replace the placeholder values with your actual configuration details** .

```env
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key         # Replace with your AWS Access Key ID
AWS_SECRET_ACCESS_KEY=your_secret_key     # Replace with your AWS Secret Access Key
AWS_S3_ENDPOINT=your_s3_endpoint         # Replace with your S3 endpoint URL
AWS_S3_BUCKET=your_s3_bucket_name        # Replace with your S3 bucket name
AWS_S3_KEY=your_s3_data_key              # Replace with the path or prefix to your S3 data
PROCESSING_DATE=your_processing_date     # Replace with the date of the data processing (e.g., '2024-10-01')

# Database Configuration
POSTGRES_DB=your_database_name           # Replace with your PostgreSQL database name
POSTGRES_USER=your_database_user         # Replace with your PostgreSQL username
POSTGRES_PASSWORD=your_database_password # Replace with your PostgreSQL password
POSTGRES_HOST=postgres                   # Default service name for PostgreSQL (do not change unless necessary)
POSTGRES_PORT=5432                       # Default PostgreSQL port (do not change unless necessary)

# Data Source
LAU_GEOJSON_URL=your_geojson_url         # Replace with the URL of your GeoJSON data source
COUNTRY_CODE=your_country_code           # Replace with the desired ISO country code (e.g., 'EE' for Estonia)
```

#### Step 3: Build and Run the Services

After setting up your `.env` file, build and start the services using Docker Compose:

```bash
docker-compose up --build
```

This command will build and start the following services:

* **PostgreSQL** with PostGIS: The database for storing spatial data.
* **ETL Service** : Responsible for extracting, transforming, and loading data into PostgreSQL.
* **FastAPI** : The API service providing endpoints to interact with the data.
* **Jupyter Notebook** : An interactive environment for data analysis and visualization.

 **⚠️ Important** : **Do not interact with any services until the ETL service has completed data processing. This may take up to 15 minutes on the first run.**

#### Step 4: Access the Services

Once the ETL service finishes processing the data, you can access the following:

* **FastAPI API Documentation** :
* [Swagger UI](http://localhost:8000/docs): Interactive API documentation.
* [ReDoc UI](http://localhost:8000/redoc): Alternative API documentation.
* **Jupyter Notebook** : Access the Jupyter interface for data analysis and visualization at [http://localhost:8888](http://localhost:8888/).

#### Step 5: Stop ETL Service (After First Run)

After the initial data processing is complete and the data is loaded into PostgreSQL, you can stop the ETL service to prevent it from running continuously.

To stop the ETL service:

```bash
docker-compose stop etl
```

This will leave PostgreSQL, FastAPI, and Jupyter Notebook running, while disabling the ETL service.

### Key API Endpoints

The following are the key API endpoints provided by FastAPI:

* **Events per Hour** : `GET /events_per_hour` — Returns the number of events grouped by hour.
* **Users per Hour** : `GET /users_per_hour` — Returns the number of unique users grouped by hour.
* **Locations per User** : `GET /locations_per_user` — Returns the number of unique locations visited by each user.
* **Unique Users per Admin Unit** : `GET /unique_users_per_admin_unit` — Returns the number of unique users per administrative unit on a given date.

### Notes

* **AWS S3** : The project integrates with AWS S3 to fetch and store data. Ensure that your credentials and bucket settings are correct.
* **PostgreSQL Setup** : The PostgreSQL service is configured with the **PostGIS** extension for managing geospatial data.
* **Initial Data Processing** : The first run of the ETL service may take up to 15 minutes depending on the size of the data.

### License

This project is licensed under the **MIT License** - see the [LICENSE](./LICENSE) file for details.

### Reporting Errors and Issues

If you encounter any errors or issues while setting up or running the project, please feel free to report them by creating an issue in the repository. Include the following details to help troubleshoot:

* A clear description of the issue.
* Any error messages or logs from the terminal.
* Steps to reproduce the issue (if possible).
* Information about your environment (OS, Docker version, etc.).

You can open an issue here: [GitHub Issues](https://github.com/0-LY/GeoDataPipeline/issues).

---

### Final Note

This project may contain errors or areas for improvement, as it is part of an ongoing learning process. Thank you for your understanding while evaluating the project.
