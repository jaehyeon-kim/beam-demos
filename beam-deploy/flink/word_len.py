import os
import re
import json
import datetime
import logging
import typing

from pyflink.common import WatermarkStrategy
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time, TimeWindow
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
)
from pyflink.common.serialization import SimpleStringSchema


def tokenize(element: str):
    for word in re.findall(r"[A-Za-z\']+", element):
        yield word


def create_message(element: typing.Tuple[str, str, float]):
    return json.dumps(dict(zip(["window_start", "window_end", "avg_len"], element)))


class AverageWindowFunction(ProcessAllWindowFunction):
    def process(
        self, context: ProcessAllWindowFunction.Context, elements: typing.Iterable[str]
    ) -> typing.Iterable[typing.Tuple[str, str, float]]:
        window: TimeWindow = context.window()
        window_start = datetime.datetime.fromtimestamp(window.start // 1000).isoformat(
            timespec="seconds"
        )
        window_end = datetime.datetime.fromtimestamp(window.end // 1000).isoformat(
            timespec="seconds"
        )
        length, count = 0, 0
        for e in elements:
            length += len(e)
            count += 1
        result = window_start, window_end, length / count if count else float("NaN")
        logging.info(f"AverageWindowFunction: result - {result}")
        yield result


def define_workflow(source_system: DataStream):
    return (
        source_system.flat_map(tokenize)
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .process(AverageWindowFunction())
    )


if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)
    env.set_parallelism(3)

    input_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"))
        .set_topics(os.getenv("INPUT_TOPIC", "input-topic"))
        .set_group_id(os.getenv("GROUP_ID", "flink-word-len"))
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    input_stream = env.from_source(
        input_source, WatermarkStrategy.no_watermarks(), "input_source"
    )

    output_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(os.getenv("BOOTSTRAP_SERVERS", "localhost:29092"))
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(os.getenv("OUTPUT_TOPIC", "output-topic-flink"))
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    define_workflow(input_stream).map(
        create_message, output_type=Types.STRING()
    ).sink_to(output_sink).name("output_sink")

    env.execute("avg-word-length-flink")
