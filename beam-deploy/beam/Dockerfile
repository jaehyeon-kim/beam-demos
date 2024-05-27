FROM flink:1.16

ARG BEAM_VERSION
ENV BEAM_VERSION=${BEAM_VERSION:-2.56.0}
ARG FLINK_VERSION
ENV FLINK_VERSION=${FLINK_VERSION:-1.16}
ARG PYTHON_VERSION
ENV PYTHON_VERSION=${PYTHON_VERSION:-3.10.13}
ENV REPO_BASE_URL=https://repo1.maven.org/maven2/org/apache/beam

## install python
RUN apt-get update -y && \
  apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
  wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
  tar -xvf Python-${PYTHON_VERSION}.tgz && \
  cd Python-${PYTHON_VERSION} && \
  ./configure --without-tests --enable-shared && \
  make -j6 && \
  make install && \
  ldconfig /usr/local/lib && \
  cd .. && rm -f Python-${PYTHON_VERSION}.tgz && rm -rf Python-${PYTHON_VERSION} && \
  ln -s /usr/local/bin/python3 /usr/local/bin/python && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

## install pip packages
RUN pip3 install apache-beam==${BEAM_VERSION}

COPY --from=apache/beam_python3.10_sdk /opt/apache/beam/ /opt/apache/beam_python/
COPY --from=apache/beam_java11_sdk:2.56.0 /opt/apache/beam/ /opt/apache/beam/
COPY --from=apache/beam_flink1.16_job_server:2.56.0  \
  /opt/apache/beam/jars/beam-runners-flink-job-server.jar /opt/apache/beam/jars/beam-runners-flink-job-server.jar
RUN wget ${REPO_BASE_URL}/beam-sdks-java-io-expansion-service/${BEAM_VERSION}/beam-sdks-java-io-expansion-service-${BEAM_VERSION}.jar \
          --progress=bar:force:noscroll -O /opt/apache/beam/jars/beam-sdks-java-io-expansion-service.jar \
  && wget ${REPO_BASE_URL}/beam-sdks-java-io-kafka/${BEAM_VERSION}/beam-sdks-java-io-kafka-${BEAM_VERSION}.jar \
          --progress=bar:force:noscroll -O /opt/apache/beam/jars/beam-sdks-java-io-kafka.jar \
  && wget ${REPO_BASE_URL}/beam-sdks-java-extensions-schemaio-expansion-service/${BEAM_VERSION}/beam-sdks-java-extensions-schemaio-expansion-service-${BEAM_VERSION}.jar \
          --progress=bar:force:noscroll -O /opt/apache/beam/jars/beam-sdks-java-extensions-schemaio-expansion-service.jar

RUN mkdir -p /opt/flink/.apache_beam/cache/jars \
  && cp /opt/apache/beam/jars/beam-runners-flink-job-server.jar /opt/flink/.apache_beam/cache/jars/beam-runners-flink-${FLINK_VERSION}-job-server-${BEAM_VERSION}.jar

COPY word_len /app/word_len
COPY word_count /app/word_count

ENV PYTHONPATH="$PYTHONPATH:/app"
