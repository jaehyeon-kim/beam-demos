import argparse
import logging

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from mydofns.synthetic_sdfn_streaming import GeneratePartitionsDoFn, MyPartition, ProcessPartitionsSplittableDoFn


def run_pipeline_streaming(beam_options):
    pipeline_options: PipelineOptions = PipelineOptions(beam_options)
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True
    with beam.pipeline.Pipeline(options=pipeline_options) as p:
        partitions: PCollection[MyPartition] = p | beam.Create([0]) | beam.ParDo(GeneratePartitionsDoFn())
        proc_chunks = partitions | beam.ParDo(ProcessPartitionsSplittableDoFn())
        proc_chunks | beam.Map(logging.info)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    my_args, beam_args = parser.parse_known_args()

    run_pipeline_streaming(beam_options=beam_args)
