FROM apache/spark-py:latest

# Switch to root for package installation
USER root

# Download compatible versions of S3A libraries and log4j
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Download PostgreSQL JDBC Driver
RUN wget -P /opt/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL source code
COPY src/ .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_SUBMIT_OPTS="-Dcom.amazonaws.sdk.disableCertChecking=true"

# Switch back to the spark user
USER ${SPARK_UID}

# Run ETL process
CMD ["spark-submit", \
     "--jars", "/opt/spark/jars/postgresql-42.6.0.jar", \
     "--driver-class-path", "/opt/spark/jars/postgresql-42.6.0.jar", \
     "--conf", "spark.hadoop.fs.s3a.endpoint.region=eu-frankfurt-1", \
     "--conf", "spark.hadoop.fs.s3a.path.style.access=true", \
     "--conf", "spark.hadoop.fs.s3a.ssl.enabled=true", \
     "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=true", \
     "--conf", "spark.hadoop.com.amazonaws.services.s3.enableV4=true", \
     "main.py"]