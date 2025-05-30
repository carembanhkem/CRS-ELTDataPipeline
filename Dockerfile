FROM quay.io/astronomer/astro-runtime:12.4.0

USER root

# Create include/output with write permissions
RUN mkdir -p /usr/local/airflow/include/output && \
    chmod -R 777 /usr/local/airflow/include/output

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER astro