FROM apache/airflow:2.6.0

# Switch to root to install system dependencies
USER root

# Install system dependencies including Java
RUN apt-get update && \
    apt-get install -y wget netcat openjdk-11-jdk procps && \
    apt-get clean

# Download Spark with Hadoop (this includes necessary Hadoop libraries)
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
RUN mkdir -p /opt/spark && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/spark --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Create and set permissions for spark-jars directory
RUN mkdir -p /opt/spark/jars && \
    wget -P /opt/spark/jars https://search.maven.org/remotecontent?filepath=org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.12.10/mongodb-driver-3.12.10.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/bson/3.12.10/bson-3.12.10.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.12.10/mongodb-driver-core-3.12.10.jar && \
    chmod -R 777 /opt/spark/jars

# Switch back to airflow user
USER airflow

# Install all required packages during image build instead of at container startup
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    pymongo==4.3.3 \
    pandas \
    matplotlib \
    seaborn

# Set environment variables for PySpark to find the jar
ENV SPARK_CLASSPATH="/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar"
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar pyspark-shell"

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3