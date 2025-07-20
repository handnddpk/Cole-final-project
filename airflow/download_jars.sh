# Download script for Iceberg and S3 dependencies
#!/bin/bash

# Create jars directory
mkdir -p /opt/bitnami/spark/jars

# Download Iceberg Spark runtime
wget -O /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

# Download AWS SDK Bundle
wget -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Download Hadoop AWS
wget -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

echo "JAR files downloaded successfully!"
