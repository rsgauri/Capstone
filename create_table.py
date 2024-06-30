from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "orbital-anchor-419817.capstone_proj.test2"
schema = [
    bigquery.SchemaField("customer_id", "INTEGER"),
    bigquery.SchemaField("region", "INTEGER"),
    bigquery.SchemaField("channel", "INTEGER"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)