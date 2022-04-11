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
runner = 'DirectRunner'  # DataflowRunner | DirectRunner
beam_options = {
    'runner': runner,
    'job_name': job_name,
    'project': PROJECT,
    'region': REGION
}
pipeline_options = PipelineOptions(**beam_options)


# %%
COLNAMES = 'Month,Average,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015'.split(
    ',')


def parse_csv(line):
    try:
        values = line.split(',')
        rowdict = {}
        for colname, value in zip(COLNAMES, values):
            rowdict[colname] = int(value)
        yield rowdict
    except:
        logging.warn('Ignoring line with {} values; expected {}',
                     len(values), len(COLNAMES))


def pull_fields(rowdict):
    result = {}
    # required string fields (see also get_output_schema)
    for col in 'INSTNM'.split(','):
        if col in rowdict:
            result[col] = rowdict[col]
        else:
            logging.info('Ignoring line missing {}', col)
            return

    # float fields (see also get_output_schema)
    for col in '2005'.split(','):
        try:
            result[col] = (float)(rowdict[col])
        except:
            result[col] = None
    yield result


# %%
# Instancio el objeto pipe que encapsula todos mis calculos
with beam.Pipeline(options=pipeline_options) as pipeline:
    raw_csv = pipeline | 'ReadCSV-fundamentals' >> beam.io.ReadFromText(
        BUCKET_DIR+input_file, skip_header_lines=1)
    data_model_stg = (
        raw_csv
        | 'parse_csv' >> beam.FlatMap(parse_csv)
        # | 'print_parsed' >> beam.Map(print)
        # | 'pull_fields' >> beam.FlatMap(pull_fields)
        | 'print_parsed' >> beam.Map(print))

    # data_model_stg = raw_csv | 'Divide fields by two' >>

# %%
