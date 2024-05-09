FROM flink:1.17

ARG PYTHON_VERSION
ENV PYTHON_VERSION=${PYTHON_VERSION:-3.10.13}
ARG BEAM_VERSION
ENV BEAM_VERSION=${FLINK_VERSION:-2.56.0}

## install java SDK
COPY --from=apache/beam_java8_sdk:2.56.0 /opt/apache/beam/ /opt/apache/beam_java/

## install python SDK
COPY --from=apache/beam_python3.8_sdk:2.56.0 /opt/apache/beam/ /opt/apache/beam_python/

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
