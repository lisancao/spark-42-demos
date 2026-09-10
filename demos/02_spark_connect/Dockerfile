# The official Apache Spark 4.2.0 image with PyArrow and pandas added.
#
# Spark 4.2 enables Arrow-optimized Python UDFs by default, and the official image does not include
# PyArrow, so Python UDFs fail on the unmodified image (VERIFIED_FACTS.md §2). The image's own
# Python 3.10 is kept, because Python UDFs require the same minor version on client and executors.
FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root
RUN python3 -m pip install --no-cache-dir "pyarrow==25.0.1" "pandas==2.3.3"
USER spark
