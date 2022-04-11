# %%
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
from apache_beam.options.value_provider import StaticValueProvider


# %%
PROJECT = 'ip-trabajo-avalino'
BUCKET = 'data_flow_21032022'
REGION = 'us-central1'
BUCKET_DIR = 'gs://data_flow_21032022'
bqtable = '2005_hurricaine_data'
bqdataset = 'dataflow'
input_file = 'hurricanes.csv'
job_name = 'dataflow-e4'
runner = 'DirectRunner'  # DataflowRunner | DirectRunner

# %%


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


class MyParseFn(beam.DoFn):

    def __init__(self, delimiter):
        self.delimiter = delimiter

    def process(self, line):
        COLNAMES = 'Month,Average,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015'.split(
            self.delimiter.get())
        if line == '':
            return
        try:
            values = line.split(self.delimiter.get())
            rowdict = {}
            for colname, value in zip(COLNAMES, values):
                rowdict[colname] = value
            yield rowdict
        except:
            logging.warn('Ignoring line with {} values; expected {}',
                         len(values), len(COLNAMES))


def main(argv=None, save_main_session=True):
    """Run the pipeline
    1) Pass arguments
    2) Read csv atending to a file delimiter
    3) Pull required fields
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default=BUCKET_DIR+'/' + input_file,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='./',
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=' + runner,
        '--project=' + PROJECT,
        '--region=' + REGION,
        '--staging_location=' + BUCKET_DIR + '/staging',
        '--temp_location=' + BUCKET_DIR + '/temp',
        '--job_name=' + job_name,
        '--template_location=gs://data_flow_21032022/templates/complex_transform_template'
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
    runtime_params = PipelineOptions().view_as(RuntimeParams)
    delimiter = runtime_params.field_delimiter.__str__()
    print(delimiter)

    # %%
    # Instancio el objeto pipe que encapsula todos mis calculos
    with beam.Pipeline(options=pipeline_options) as pipeline:
        raw_csv = (
            pipeline
            | 'ReadCSV-fundamentals' >> beam.io.ReadFromText(
                known_args.input, skip_header_lines=1)
            | 'parse_csv' >> beam.ParDo(MyParseFn(runtime_params.field_delimiter))
            | WriteToText("gs://data_flow_21032022/out"))
        # | 'print_parsed' >> beam.Map(print))
        # | 'parse_csv' >> beam.ParDo(MyParseFn(';'))
        # | 'print_parsed' >> beam.Map(print))
        # | 'pull_fields and divide' >> beam.FlatMap(pull_fields)
        # | 'write_bq' >> beam.io.gcp.bigquery.WriteToBigQuery(
        #     bqtable, bqdataset, schema='Month: string, 2005: FLOAT64, 2006: FLOAT64'))

    # data_model_stg = raw_csv | 'Divide fields by two' >>


# Para ejecutar la funcion main y devolver logs
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
