import os
import re
import logging
import typing

from pyflink.common import WatermarkStrategy
from pyflink.datastream import (
    DataStream,
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
)
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import SlidingEventTimeWindows, Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema


def tokenize(element: str):
    for word in re.findall(r"[A-Za-z\']+", element):
        yield word


class WordAccum(typing.NamedTuple):
    length: int
    count: int


class AverageFn(AggregateFunction):
    def create_accumulator(self):
        return WordAccum(length=0, count=0)

    def add(self, value: WordAccum, accumulator: WordAccum):
        length, count = tuple(accumulator)
        return WordAccum(length=length + len(value), count=count + 1)

    def merge(self, acc_a: WordAccum, acc_b: WordAccum):
        return WordAccum(
            length=acc_a.length + acc_b.length, count=acc_a.count + acc_b.count
        )

    def get_result(self, accumulator: WordAccum):
        length, count = tuple(accumulator)
        return length / count if count else float("NaN")


def define_workflow(source_system: DataStream):
    return (
        source_system.flat_map(tokenize)
        .window_all(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
        .aggregate(
            AverageFn(),
            output_type=Types.FLOAT(),
        )
    )


RUNTIME_ENV = os.getenv("RUNTIME_ENV", "local")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "input-topic")
GROUP_ID = os.getenv("GROUP_ID", "flink-word-len")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d:%(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(
        f"RUNTIME_ENV - {RUNTIME_ENV}, BOOTSTRAP_SERVERS - {BOOTSTRAP_SERVERS}"
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)
    # env.set_parallelism(5)
    if RUNTIME_ENV == "local":
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        jar_files = ["flink-sql-connector-kafka-1.16.2.jar"]
        jar_paths = tuple(
            [f"file://{os.path.join(CURRENT_DIR, name)}" for name in jar_files]
        )
        logging.info(f"adding local jars - {', '.join(jar_files)}")
        env.add_jars(*jar_paths)

    input_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    input_stream = env.from_source(
        input_source, WatermarkStrategy.no_watermarks(), "input_source"
    )

    output_stream = define_workflow(input_stream)

    output_stream.print()

    env.execute("avg-word-length-flink")
