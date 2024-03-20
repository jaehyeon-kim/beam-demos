#!/usr/bin/env bash

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -k|--kafka) start_kafka=true;;
        -f|--flink) start_flink=true;;
        -a|--all) start_all=true;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ ! -z $start_all ] &&  [ $start_all = true ]; then
  start_kafka=true
  start_flink=true
fi
# echo "start_all? ${start_all} start kakfa? ${start_kafka} start flink? ${start_flink}"

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

#### start kafka cluster in docker
if [ ! -z $start_kafka ] &&  [ $start_kafka = true ]; then
  docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up -d
fi

#### start local flink cluster
## 1. download flink binary and decompress in the same folder
##  wget https://dlcdn.apache.org/flink/flink-1.16.3/flink-1.16.3-bin-scala_2.12.tgz
##  tar -zxf flink-1.16.3-bin-scala_2.12.tgz
## 2. update flink configuration in eg) ./flink-1.16.3/config/flink-conf.yaml
##  rest.port: 8081                    # uncommented
##  rest.address: localhost            # kept as is
##  rest.bind-address: 0.0.0.0         # changed from localhost
##  taskmanager.numberOfTaskSlots: 10  # updated from 1
if [ ! -z $start_flink ] && [ $start_flink = true ]; then
  ${SCRIPT_DIR}/flink-1.16.3/bin/start-cluster.sh
fi
