import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam import pvalue, Windowing
from apache_beam.io import kafka
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.window import (
    GlobalWindows,
    BoundedWindow,
    FixedWindows,
)
from apache_beam.transforms.util import Reify
from apache_beam.utils.timestamp import Timestamp
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

MAIN_OUTPUT = "main_output"
DROPPABLE_OUTPUT = "droppable_output"


class SplitDroppable(beam.PTransform):
    def expand(self, pcoll):
        windowing: Windowing = pcoll.windowing
        assert windowing.windowfn != GlobalWindows
        # print(windowing)
        # print(windowing.accumulation_mode)

        def to_kv(
            element: typing.Tuple[str, Timestamp, BoundedWindow],
        ) -> typing.Tuple[str, str]:
            value, timestamp, window = element
            return str(window), value

        outputs: pvalue.DoOutputsTuple = (
            pcoll
            | Reify.Window()
            | beam.Map(to_kv)
            | beam.WindowInto(GlobalWindows())
            | beam.ParDo(SplitDroppableDataFn(windowing=windowing))
            .with_outputs(DROPPABLE_OUTPUT, main=MAIN_OUTPUT)
            .with_input_types(typing.Tuple[str, str])
        )

        pcolls = {}
        pcolls[MAIN_OUTPUT] = outputs[MAIN_OUTPUT]
        pcolls[DROPPABLE_OUTPUT] = outputs[DROPPABLE_OUTPUT]

        return pcolls | Rewindow(windowing=windowing)


class Rewindow(beam.PTransform):
    def __init__(self, label: str | None = None, windowing: Windowing = None):
        super().__init__(label)
        self.windowing = windowing

    def expand(self, pcolls):
        window_fn = self.windowing.windowfn
        allowed_lateness = self.windowing.allowed_lateness
        # closing_behavior = self.windowing.closing_behavior # emit always
        # on_time_behavior = self.windowing.on_time_behavior # fire always
        timestamp_combiner = self.windowing.timestamp_combiner
        trigger_fn = self.windowing.triggerfn
        accumulation_mode = (
            AccumulationMode.DISCARDING
            if self.windowing.accumulation_mode == 1
            else AccumulationMode.ACCUMULATING
        )
        main_output = pcolls[MAIN_OUTPUT] | beam.WindowInto(
            windowfn=window_fn,
            trigger=trigger_fn,
            accumulation_mode=accumulation_mode,
            timestamp_combiner=timestamp_combiner,
            allowed_lateness=allowed_lateness,
        )
        return {
            "main_output": main_output,
            "droppable_output": pcolls[DROPPABLE_OUTPUT],
        }


class SplitDroppableDataFn(beam.DoFn):
    TOO_LATE = ReadModifyWriteStateSpec("too_late", beam.coders.BooleanCoder())
    WINDOW_GC_TIMER = TimerSpec("window_gc_timer", TimeDomain.WATERMARK)

    def __init__(self, windowing: Windowing):
        self.windowing = windowing

    def process(
        self,
        element: typing.Tuple[str, str],
        too_late=beam.DoFn.StateParam(TOO_LATE),
        window_gc_timer=beam.DoFn.TimerParam(WINDOW_GC_TIMER),
    ):
        too_late_for_window = too_late.read() or False
        if too_late_for_window is False:
            window_gc_timer.set(
                self.get_max_ts(element[0])
                + self.windowing.allowed_lateness.micros // 1000000
            )
        if too_late_for_window is True:
            yield pvalue.TaggedOutput(DROPPABLE_OUTPUT, element[1])
        else:
            yield element[1]

    @on_timer(WINDOW_GC_TIMER)
    def on_window_gc_timer(self, too_late=beam.DoFn.StateParam(TOO_LATE)):
        too_late.write(True)

    @staticmethod
    def get_max_ts(window_str: str):
        """Extract the maximum timestamp of a window string eg) '[0.001, 600.001)'"""
        bounds = re.findall(r"[\d]+[.\d]+", window_str)
        assert len(bounds) == 2
        return float(bounds[1])


def decode_message(kafka_kv: tuple):
    print(kafka_kv)
    return kafka_kv[1].decode("utf-8")


def tokenize(element: str):
    return re.findall(r"[A-Za-z\']+", element)


def create_main_message(element: typing.Tuple[Timestamp, Timestamp, str]):
    msg = json.dumps(
        {
            "start": element[0].to_utc_datetime().isoformat(timespec="seconds"),
            "end": element[1].to_utc_datetime().isoformat(timespec="seconds"),
            "word": element[2],
        }
    )
    print(msg)
    return element[2].encode("utf-8"), msg.encode("utf-8")


def create_droppable_message(element: str):
    print(element)
    return element.encode("utf-8"), element.encode("utf-8")


class AddWindowTS(beam.DoFn):
    def process(self, element: str, win_param=beam.DoFn.WindowParam):
        yield (win_param.start, win_param.end, element)


def run():
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument("--runner", default="FlinkRunner", help="Apache Beam runner")
    parser.add_argument(
        "--use_own",
        action="store_true",
        default="Flag to indicate whether to use an own local cluster",
    )
    parser.add_argument(
        "--batch_size", type=int, default=10, help="Batch size to process"
    )
    parser.add_argument(
        "--max_wait_secs",
        type=int,
        default=4,
        help="Maximum wait seconds before processing",
    )
    parser.add_argument("--input", default="input-topic", help="Input topic")
    parser.add_argument(
        "--job_name",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Job name",
    )
    opts = parser.parse_args()
    print(opts)

    pipeline_opts = {
        "runner": opts.runner,
        "job_name": opts.job_name,
        "environment_type": "LOOPBACK",
        "streaming": True,
        "parallelism": 3,
        "experiments": [
            "use_deprecated_read"
        ],  ## https://github.com/apache/beam/issues/20979
        "checkpointing_interval": "60000",
    }
    if opts.use_own is True:
        pipeline_opts = {**pipeline_opts, **{"flink_master": "localhost:8081"}}
    print(pipeline_opts)
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)
    outputs = (
        p
        | "Read from Kafka"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                ),
                "auto.offset.reset": "earliest",
                # "enable.auto.commit": "true",
                "group.id": opts.job_name,
            },
            topics=[opts.input],
            timestamp_policy=kafka.ReadFromKafka.create_time_policy,
        )
        | "Decode messages" >> beam.Map(decode_message)
        | "Extract words" >> beam.FlatMap(tokenize)
        | "Windowing"
        >> beam.WindowInto(
            FixedWindows(10 * 60),
            allowed_lateness=30,
            accumulation_mode=AccumulationMode.DISCARDING,
        )
        | "Spilt droppable" >> SplitDroppable()
    )

    (
        outputs[MAIN_OUTPUT]
        | "Add window timestamp" >> beam.ParDo(AddWindowTS())
        | "Create main message"
        >> beam.Map(create_main_message).with_output_types(typing.Tuple[bytes, bytes])
        | "Write to main topic"
        >> kafka.WriteToKafka(
            producer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                )
            },
            topic=f"{opts.job_name}_m",
        )
    )

    (
        outputs[DROPPABLE_OUTPUT]
        | "Create droppable message"
        >> beam.Map(create_droppable_message).with_output_types(
            typing.Tuple[bytes, bytes]
        )
        | "Write to droppable topic"
        >> kafka.WriteToKafka(
            producer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS",
                    "host.docker.internal:29092",
                )
            },
            topic=f"{opts.job_name}_d",
        )
    )

    logging.getLogger().setLevel(logging.WARN)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
