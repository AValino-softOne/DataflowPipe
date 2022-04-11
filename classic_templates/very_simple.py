import apache_beam as beam
import argparse
import logging
import re
import logging
import datetime
import os
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class RuntimeParams(PipelineOptions):
    """Runtime parameters for template execution

    Args:
        PipelineOptions (_type_): _description_
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--field_delimiter',
            type=str,
            help='Field delimiter')


class SplitWords(beam.DoFn):
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, text):
        for word in text.split(self.delimiter):
            yield word


runtime_params = PipelineOptions().view_as(RuntimeParams)

pipeline_options = PipelineOptions()
# Save main session state so pickled functions and classes
# defined in __main__ can be unpickled
pipeline_options.view_as(SetupOptions).save_main_session = True
delimiter = runtime_params.field_delimiter.__str__()
print(delimiter)


with beam.Pipeline(options=pipeline_options) as pipeline:
    plants = (
        pipeline
        | 'Gardening plants' >> beam.Create([
            '🍓Strawberry,🥕Carrot,🍆Eggplant',
            '🍅Tomato,🥔Potato',
        ])
        | 'Split words' >> beam.ParDo(SplitWords(delimiter))
        | beam.Map(print))
