from googleapiclient.discovery import build

def trigger_df_job(cloud_event, environment):
    service = build('dataflow', 'v1b3')
    project = "orbital-anchor-419817"
    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"
    template_body = {
        "jobName": "beam-bq-job-72", 
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://capstone_proj_metadata/cust.js",
            "JSONPath": "gs://capstone_proj_metadata/cust.json",
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "orbital-anchor-419817:capstone_proj.customer",
            "inputFilePattern": "gs://capstone_proj_9/customers.csv",
            "bigQueryLoadingTemporaryDirectory": "gs://capstone_proj_metadata/"
        }
    }
    request = service.projects().templates().launch(projectId=project, gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)


