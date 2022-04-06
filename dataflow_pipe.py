"""Dataflow Pipe in python code.
1) Read CSV file
2) Divide al rows by a number
3) Divide al fields by a delimiter that we will pass as a parameter
4) Of all fields discard header

"""
# %%

import os
import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Lets load the initial variables
# %%
PROJECT = 'ip-trabajo-avalino'
BUCKET = 'data_flow_21032022'
REGION = 'us-central1'
BUCKET_DIR = 'gs://data_flow_21032022'
input_file = '/hurricanes.csv'
job_name = 'dataflow-fundamentals'
runner = 'DataflowRunner'  # DataflowRunner | DirectRunner
beam_options = {
    'runner': runner,
    'job_name': job_name,
    'project': PROJECT,
    'region': REGION
}
pipeline_options = PipelineOptions(**beam_options)


# %%
# Instancio el objeto pipe que encapsula todos mis calculos
with beam.Pipeline(options=pipeline_options) as pipeline:
    raw_csv = pipeline | 'ReadCSV-fundamentals' >> beam.io.ReadFromText(
        BUCKET_DIR+input_file)

# opts = beam.Pipeline.PipelineOptions(flags=[], **options)

# %%
