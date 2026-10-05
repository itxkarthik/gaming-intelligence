FROM apache/spark:3.5.1

USER root

# Python streaming/ML/test deps, pinned in requirements.txt for the image's
# Python 3.8 (pyspark + py4j come with the image).
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Kafka connector baked into the image: spark-submit needs no --packages,
# so jobs start without network access and parallel submits cannot race on
# a shared Ivy cache. These are the 4 jars spark-sql-kafka-0-10_2.12:3.5.1
# resolves that the Spark 3.5.1 image does not already ship.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

USER spark
