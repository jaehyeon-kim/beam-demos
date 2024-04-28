import argparse
import logging

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

from mydofns.synthetic_sdfn_batch import MyFile, GenerateFilesDoFn, ProcessFilesSplittableDoFn


def run_pipeline_batch(beam_options):
    pipeline_options: PipelineOptions = PipelineOptions(beam_options)
    with beam.pipeline.Pipeline(options=pipeline_options) as p:
        files: PCollection[MyFile] = p | beam.Create([0]) | beam.ParDo(GenerateFilesDoFn())
        proc_chunks = files | beam.ParDo(ProcessFilesSplittableDoFn())
        proc_chunks | beam.Map(logging.info)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    my_args, beam_args = parser.parse_known_args()

    run_pipeline_batch(beam_options=beam_args)
