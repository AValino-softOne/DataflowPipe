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
import shutil
import os
import subprocess


# Lets load the initial variables
# %%
PROJECT = 'ip-trabajo-avalino'
BUCKET = 'data_flow_21032022'
REGION = 'us-central1'
BUCKET_DIR = 'gs://data_flow_21032022'
bqtable = '2005_hurricaine_data'
bqdataset = 'dataflow'
input_file = '/hurricanes.csv'
job_name = 'dataflow-e1'
runner = 'DataflowRunner'  # DataflowRunner | DirectRunner
beam_options = {
    'runner': runner,
    'job_name': job_name,
    'project': PROJECT,
    'region': REGION,
    'temp_location': 'gs://data_flow_21032022/temp'
}
pipeline_options = PipelineOptions(**beam_options)


# %%


def parse_csv(line):
    COLNAMES = 'Month,Average,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015'.split(
        ',')
    if line == '':
        return
    try:
        values = line.split(',')
        rowdict = {}
        for colname, value in zip(COLNAMES, values):
            rowdict[colname] = value
        yield rowdict
    except:
        logging.warn('Ignoring line with {} values; expected {}',
                     len(values), len(COLNAMES))


def pull_fields(rowdict):
    result = {}
    # required string fields (see also get_output_schema)
    for col in 'Month'.split(','):
        if col in rowdict:
            result[col] = rowdict[col]
        else:
            logging.info('Ignoring line missing {}', col)
            return

    # float fields (see also get_output_schema)
    for col in '2005,2006'.split(','):
        try:
            result[col] = (float)(rowdict[col])/2
        except:
            result[col] = None
    yield result


def get_output_schema():
    result = 'Month:string'
    for col in '2005,2006'.split(','):
        result = result + ',' + col + ':FLOAT64'
    print(result)
    return result


# %%
# Instancio el objeto pipe que encapsula todos mis calculos
with beam.Pipeline(options=pipeline_options) as pipeline:
    raw_csv = (
        pipeline
        | 'ReadCSV-fundamentals' >> beam.io.ReadFromText(
            BUCKET_DIR+input_file, skip_header_lines=1)
        | 'parse_csv' >> beam.FlatMap(parse_csv)
        # | 'print_parsed' >> beam.Map(print)
        | 'pull_fields and divide' >> beam.FlatMap(pull_fields)
        # | 'print_parsed' >> beam.Map(print)
        | 'write_bq' >> beam.io.gcp.bigquery.WriteToBigQuery(
            bqtable, bqdataset, schema='Month: string, 2005: FLOAT64, 2006: FLOAT64'))

    # data_model_stg = raw_csv | 'Divide fields by two' >>

# %%
