FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYENV_ROOT="/root/.pyenv"
ENV PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"

# Minimal system dependencies required by make setup
RUN apt-get update && apt-get install -y \
    make \
    curl \
    git \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    wget \
    llvm \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev \
    openjdk-17-jdk \
    python3-pip

# Install pyenv
RUN curl https://pyenv.run | bash

# Set working directory and copy project
WORKDIR /app
COPY . .

# Spark GCS JAR
RUN mkdir -p /opt/spark/jars/
COPY ./jars/gcs-connector-hadoop3-2.2.11-shaded.jar /opt/spark/jars/

# Entrypoint leaves setup to Kubernetes command
CMD ["/bin/bash"]
