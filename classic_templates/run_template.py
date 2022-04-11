from googleapiclient.discovery import build
import logging


# project = 'your-gcp-project'
# job = 'unique-job-name'
# template = 'gs://dataflow-templates/latest/Word_Count'
# parameters = {
#     'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
#     'output': 'gs://<your-gcs-bucket>/wordcount/outputs',
# }

dataflow = build('dataflow', 'v1b3')
request = dataflow.projects().templates().launch(
    projectId='ip-trabajo-avalino',
    gcsPath='gs://data_flow_21032022/templates/complex_transform_template',
    body={
        'jobName': 'parse_from_apiclient_e2',
        'parameters': {
            'field_delimiter': ','
        }
    }
)

response = request.execute()
