FROM apache/airflow:2.6.0

# 1) Switch to root to install system dependencies
USER root

RUN apt-get update && \
    apt-get install -y wget netcat openjdk-11-jdk procps unzip xvfb curl gnupg && \
    apt-get clean

# 2) Install Chrome runtime dependencies
RUN apt-get update && \
    apt-get install -y libxi6 libgconf-2-4 fonts-liberation libasound2 \
                       libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
                       libgdk-pixbuf2.0-0 libnspr4 libnss3 && \
    apt-get clean

# 3) Add Google Chrome repo and install latest Chrome (v136.0.7103.92)
RUN wget -q -O /tmp/linux_signing_key.pub \
        https://dl-ssl.google.com/linux/linux_signing_key.pub && \
    apt-key add /tmp/linux_signing_key.pub && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
        > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    apt-get clean

# 4) Dynamically fetch and install matching ChromeDriver
RUN CHROME_VER=$(google-chrome --version | awk '{print $3}') && \
    echo "Detected Chrome version: $CHROME_VER" && \
    wget -q -O /tmp/chromedriver.zip \
      "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VER}/linux64/chromedriver-linux64.zip" && \
    mkdir -p /tmp/chromedriver && \
    unzip -q /tmp/chromedriver.zip -d /tmp/chromedriver && \
    mv /tmp/chromedriver/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver /tmp/chromedriver.zip

# 5) Download Spark with Hadoop
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
RUN mkdir -p /opt/spark && \
    wget -q \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
        -C /opt/spark --strip-components=1 && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 6) Add MongoDB Spark connector JARs
RUN mkdir -p /opt/spark/jars && \
    wget -P /opt/spark/jars \
      https://search.maven.org/remotecontent?filepath=org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.12.10/mongodb-driver-3.12.10.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/bson/3.12.10/bson-3.12.10.jar && \
    wget -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.12.10/mongodb-driver-core-3.12.10.jar && \
    chmod -R 777 /opt/spark/jars

# 7) Switch back to the airflow user
USER airflow

# 8) Copy data file for your DAG
COPY emiten.csv /opt/airflow/emiten.csv

# 9) Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    pymongo==4.3.3 \
    pandas \
    matplotlib \
    seaborn \
    selenium \
    webdriver-manager>=4.0.0 \
    chromedriver-py \
    beautifulsoup4 \
    requests \
    lxml

# 10) Set environment variables for Spark & Java
ENV SPARK_CLASSPATH="/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar"
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar pyspark-shell"
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3