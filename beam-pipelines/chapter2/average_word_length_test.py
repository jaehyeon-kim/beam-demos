import sys
import unittest

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AfterCount, AccumulationMode, Repeatedly
from apache_beam.transforms.window import GlobalWindows
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from average_word_length import tokenize, AverageFn


def main(out=sys.stderr, verbosity=2):
    loader = unittest.TestLoader()

    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity=verbosity).run(suite)


class MaxWordLengthTest(unittest.TestCase):
    def test_windowing_behaviour(self):
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            """
               We should put each element separately. The reason we do this is to ensure 
               that our trigger will be invoked in between each element. Because the trigger 
               invocation is optional, a runner might skip a particular firing. Putting each element 
               separately into addElements makes DirectRunner (our testing runner) invokes a trigger 
               for each input element.
            """
            test_stream = (
                TestStream(coder=coders.StrUtf8Coder())
                .with_output_types(str)
                .add_elements(["a"])
                .add_elements(["bb"])
                .add_elements(["ccc"])
                .advance_watermark_to_infinity()
            )

            output = (
                p
                | test_stream
                | "Windowing"
                >> beam.WindowInto(
                    GlobalWindows(),
                    trigger=Repeatedly(AfterCount(1)),
                    allowed_lateness=0,
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                )
                | "Extract words" >> beam.FlatMap(tokenize)
                | "Get avg word length" >> beam.CombineGlobally(AverageFn()).without_defaults()
            )

            EXPECTED_OUTPUT = [1.0, 1.5, 2.0]

            assert_that(output, equal_to(EXPECTED_OUTPUT))


if __name__ == "__main__":
    main(out=None)
