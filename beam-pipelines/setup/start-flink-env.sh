#!/usr/bin/env bash

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -k|--kafka) start_kafka=true;;
        -f|--flink) start_flink=true;;
        -g|--grpc) start_grpc=true;;
        -a|--all) start_all=true;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ ! -z $start_all ] &&  [ $start_all = true ]; then
  start_kafka=true
  start_flink=true
  start_grpc=true
fi

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

#### start kafka cluster in docker
if [ ! -z $start_kafka ] &&  [ $start_kafka = true ]; then
  docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up -d
fi

#### start grpc server in docker
if [ ! -z $start_grpc ] &&  [ $start_grpc = true ]; then
  docker-compose -f ${SCRIPT_DIR}/docker-compose-grpc.yml up -d
fi

#### start local flink cluster
## 0. install java
## 1. download flink binary and decompress in the same folder
##  FLINK_VERSION=1.18.1
##  wget https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz
##  tar -zxf flink-${FLINK_VERSION}-bin-scala_2.12.tgz
## 2. update flink configuration in eg) ./flink-${FLINK_VERSION}/conf/flink-conf.yaml
##  rest.port: 8081                    # uncommented
##  rest.address: localhost            # kept as is
##  rest.bind-address: 0.0.0.0         # changed from localhost
##  taskmanager.numberOfTaskSlots: 10  # updated from 1
## 3. update file permission recursively
##  chmod -R +x flink-${FLINK_VERSION}/bin
if [ ! -z $start_flink ] && [ $start_flink = true ]; then
  FLINK_VERSION=1.18.1
  ${SCRIPT_DIR}/flink-${FLINK_VERSION}/bin/start-cluster.sh
fi
