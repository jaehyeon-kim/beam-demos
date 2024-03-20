#!/usr/bin/env bash

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -k|--kafka) stop_kafka=true;;
        -f|--flink) stop_flink=true;;
        -a|--all) stop_all=true;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

if [ ! -z $stop_all ] && [ $stop_all = true ]; then
  stop_kafka=true
  stop_flink=true
fi
# echo "stop all? ${stop_all} stop kakfa? ${stop_kafka} stop flink? ${stop_flink}"

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

#### stop kafka cluster in docker
if [ ! -z $stop_kafka ] && [ $stop_kafka = true ]; then
    docker-compose -f ${SCRIPT_DIR}/docker-compose.yml down -v
fi

#### stop local flink cluster
if [ ! -z $stop_flink ] && [ $stop_flink = true ]; then
    ${SCRIPT_DIR}/flink-1.16.3/bin/stop-cluster.sh
fi

#### remove all stopped containers
docker container prune -f
