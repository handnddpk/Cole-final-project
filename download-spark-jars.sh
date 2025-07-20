#!/bin/bash
# Script to download required JAR files for Spark

echo "Downloading Spark JAR files..."

# Create jars directory if it doesn't exist
mkdir -p airflow/jars

# Function to download JAR if it doesn't exist
download_jar() {
    local url=$1
    local filename=$2
    
    if [ ! -f "airflow/jars/${filename}" ]; then
        echo "Downloading ${filename}..."
        curl -L -o "airflow/jars/${filename}" "${url}" || {
            echo "Failed to download ${filename}"
            return 1
        }
        echo "Downloaded ${filename}"
    else
        echo "${filename} already exists"
    fi
}

# Download required JAR files
echo "Checking and downloading required JAR files..."

# Iceberg JAR files
download_jar "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" "iceberg-spark-runtime-3.5_2.12-1.4.2.jar"

# AWS SDK JAR files for S3 support
download_jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" "hadoop-aws-3.3.4.jar"
download_jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" "aws-java-sdk-bundle-1.12.367.jar"

echo "JAR files download completed!"
