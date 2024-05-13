FROM flink:1.17

ARG PYTHON_VERSION
ENV PYTHON_VERSION=${PYTHON_VERSION:-3.10.13}
ARG FLINK_VERSION
ENV FLINK_VERSION=${BEAM_VERSION:-1.17}
ARG BEAM_VERSION
ENV BEAM_VERSION=${BEAM_VERSION:-2.56.0}

## add python/java SDKs and job server
COPY --from=apache/beam_python3.10_sdk:2.56.0 /opt/apache/beam/ /opt/apache/beam_python/
COPY --from=apache/beam_java11_sdk:2.56.0 /opt/apache/beam/ /opt/apache/beam/
COPY --from=apache/beam_flink1.17_job_server:2.56.0  \
  /opt/apache/beam/jars/beam-runners-flink-job-server.jar /opt/apache/beam/jars/beam-runners-flink-job-server.jar
RUN chown -R flink:flink /opt/apache/beam_python && chown -R flink:flink /opt/apache/beam

## cache job server jar
RUN mkdir -p /opt/flink/.apache_beam/cache/jars \
  && cp /opt/apache/beam/jars/beam-runners-flink-job-server.jar /opt/flink/.apache_beam/cache/jars/beam-runners-flink-${FLINK_VERSION}-job-server-${BEAM_VERSION}.jar \
  && chown -R flink:flink /opt/flink/.apache_beam

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

## copy pipeline source
USER flink
RUN mkdir /opt/flink/usrlib
ADD word_len.py /opt/flink/usrlib/word_len.py
