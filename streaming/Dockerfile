FROM apache/spark:3.5.1

USER root

# Python streaming/ML/test deps, pinned in requirements.txt for the image's
# Python 3.8 (pyspark + py4j come with the image).
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Ivy cache for `spark-submit --packages`, writable by the spark user.
# (/tmp/spark-checkpoints is a bind mount of ./data/checkpoints — see
# docker-compose.yml — so it is not created here.)
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark && \
    chmod -R 777 /home/spark

USER spark
