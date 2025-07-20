#!/bin/bash
# Spark initialization script to download required JAR files

echo "Downloading required JAR files for Spark..."

# Create jars directory if it doesn't exist
mkdir -p /opt/bitnami/spark/jars

# Download Iceberg Spark runtime
if [ ! -f "/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar" ]; then
    echo "Downloading Iceberg Spark runtime..."
    wget -q -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar || echo "Failed to download Iceberg runtime"
fi

# Download AWS SDK Bundle
if [ ! -f "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "Downloading AWS SDK Bundle..."
    wget -q -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar || echo "Failed to download AWS SDK"
fi

# Download Hadoop AWS
if [ ! -f "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar" ]; then
    echo "Downloading Hadoop AWS..."
    wget -q -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar || echo "Failed to download Hadoop AWS"
fi

echo "JAR files downloaded successfully!"

# Start the original Spark process
echo "Starting Spark with arguments: $@"
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@"
