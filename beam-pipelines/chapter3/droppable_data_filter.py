import os
import argparse
import json
import re
import typing
import logging

import apache_beam as beam
from apache_beam import pvalue, Windowing
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

from io_utils import ReadWordsFromKafka, WriteOutputsToKafka

MAIN_OUTPUT = "main_output"
DROPPABLE_OUTPUT = "droppable_output"


def create_message(
    element: typing.Union[typing.Tuple[Timestamp, Timestamp, str], str], is_main: bool
):
    if is_main:
        msg = json.dumps(
            {
                "start": element[0].seconds(),
                "end": element[1].seconds(),
                "word": element[2],
            }
        )
        key = element[2]
    else:
        msg = element
        key = msg
    logging.info(f"{'main' if is_main else 'droppable'} message - {msg}")
    return key.encode("utf-8"), msg.encode("utf-8")


class SplitDroppable(beam.PTransform):
    def expand(self, pcoll):
        windowing: Windowing = pcoll.windowing
        assert windowing.windowfn != GlobalWindows

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
        max_ts = self.get_max_ts(element[0])
        allowed_lateness_sec = self.windowing.allowed_lateness.micros // 1000000
        too_late_for_window = too_late.read() or False
        logging.info(f"string (value) - {element[1]}, window (key) {element[0]}")
        if too_late_for_window is False:
            timer_val = max_ts + allowed_lateness_sec
            logging.info(f"set up eow timer at {timer_val}")
            window_gc_timer.set(timer_val)
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
        main_output = pcolls[MAIN_OUTPUT] | "MainWindowInto" >> beam.WindowInto(
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


class AddWindowTS(beam.DoFn):
    def process(self, element: str, win_param=beam.DoFn.WindowParam):
        yield (win_param.start, win_param.end, element)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    parser.add_argument(
        "--bootstrap_servers",
        default="host.docker.internal:29092",
        help="Kafka bootstrap server addresses",
    )
    parser.add_argument("--input_topic", default="input-topic", help="Input topic")
    parser.add_argument("--window_length", default=5, type=int, help="Input topic")
    parser.add_argument("--allowed_lateness", default=2, type=int, help="Input topic")
    parser.add_argument(
        "--output_topic",
        default=re.sub("_", "-", re.sub(".py$", "", os.path.basename(__file__))),
        help="Output topic",
    )
    parser.add_argument(
        "--deprecated_read",
        action="store_true",
        default="Whether to use a deprecated read. See https://github.com/apache/beam/issues/20979",
    )
    parser.set_defaults(deprecated_read=False)

    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        outputs = (
            p
            | "ReadInputsFromKafka"
            >> ReadWordsFromKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topics=[known_args.input_topic],
                group_id=f"{known_args.output_topic}-group",
                deprecated_read=known_args.deprecated_read,
            )
            | "Windowing"
            >> beam.WindowInto(
                FixedWindows(known_args.window_length),
                allowed_lateness=known_args.allowed_lateness,
                accumulation_mode=AccumulationMode.DISCARDING,
            )
            | "SpiltDroppable" >> SplitDroppable()
        )

        (
            outputs[MAIN_OUTPUT]
            | "AddWindowTimestamp" >> beam.ParDo(AddWindowTS())
            | "CreateMainMessage"
            >> beam.Map(create_message, is_main=True).with_output_types(
                typing.Tuple[bytes, bytes]
            )
            | "WriteToMainTopic"
            >> WriteOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic="output-normal-topic",
                deprecated_read=known_args.deprecated_read,
            )
        )

        (
            outputs[DROPPABLE_OUTPUT]
            | "CreateDroppableMessage"
            >> beam.Map(create_message, is_main=False).with_output_types(
                typing.Tuple[bytes, bytes]
            )
            | "WriteToDroppableTopic"
            >> WriteOutputsToKafka(
                bootstrap_servers=known_args.bootstrap_servers,
                topic="output-droppable-topic",
                deprecated_read=known_args.deprecated_read,  # TO DO: remove as it applies only to ReadFromKafka
            )
        )

        logging.getLogger().setLevel(logging.INFO)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    run()
