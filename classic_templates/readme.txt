To upload de template to cloud storage

  python -m examples.mymodule \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --staging_location gs://BUCKET_NAME/staging \
    --temp_location gs://BUCKET_NAME/temp \
    --template_location gs://BUCKET_NAME/templates/TEMPLATE_NAME \
    --region REGION
    
    
    
  To run the template using pytyhon libraries
  
  from googleapiclient.discovery import build

# project = 'your-gcp-project'
# job = 'unique-job-name'
# template = 'gs://dataflow-templates/latest/Word_Count'
# parameters = {
#     'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
#     'output': 'gs://<your-gcs-bucket>/wordcount/outputs',
# }

dataflow = build('dataflow', 'v1b3')
request = dataflow.projects().templates().launch(
    projectId=project,
    gcsPath=template,
    body={
        'jobName': job,
        'parameters': parameters,
    }
)

response = request.execute()
  
