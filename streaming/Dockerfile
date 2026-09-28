FROM apache/spark:3.5.1

USER root

# Install Python streaming and ML dependencies compatible with Python 3.8
RUN pip3 install --no-cache-dir \
    redis==5.0.4 \
    "pandas>=1.5.0,<2.1.0" \
    "numpy>=1.22.0,<1.25.0" \
    "scikit-learn>=1.2.0,<1.4.0" \
    pytest

# Pre-create Ivy cache and checkpoint directories with permissions for spark user
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars /tmp/spark-checkpoints && \
    chown -R spark:spark /home/spark /tmp/spark-checkpoints && \
    chmod -R 777 /home/spark /tmp/spark-checkpoints

USER spark
