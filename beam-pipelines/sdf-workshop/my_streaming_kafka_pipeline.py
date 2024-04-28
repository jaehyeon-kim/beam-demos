import argparse
import logging

from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

from typing import List
import apache_beam as beam

from mydofns.kafka_sdfn_streaming import ReadPartitionsDoFn, ProcessKafkaPartitionsDoFn

TOPIC = "beam-topic"


def run_pipeline(topic: str, bootstrap_server: List[str], beam_options):
    pipeline_options: PipelineOptions = PipelineOptions(beam_options)

    with beam.Pipeline(options=pipeline_options) as p:
        ps: PCollection[int] = p | beam.Create([1]) | "Check partitions" >> beam.ParDo(
            ReadPartitionsDoFn(topic, bootstrap_server))

        msgs: PCollection[str] = ps | "Read from Kafka" >> beam.ParDo(
            ProcessKafkaPartitionsDoFn(topic, bootstrap_server))

        msgs | beam.Map(lambda x: logging.info(x))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument("--bootstrap", type=str, required=False)
    parser.add_argument("--topic", type=str, required=False, default=TOPIC)

    my_args, beam_args = parser.parse_known_args()

    run_pipeline(topic=my_args.topic,
                 bootstrap_server=my_args.bootstrap,
                 beam_options=beam_args)
