from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """Uploads a file to Google Cloud Storage."""
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Define the blob (object) name in the bucket
    blob = bucket.blob(destination_blob_name)

    # Upload the local file to the defined blob
    blob.upload_from_filename(local_file_path)

    print(f"File {local_file_path} uploaded to {bucket_name}/{destination_blob_name}.")

if __name__ == "__main__":
    # Update these variables with your values
    local_file_path = "Dataset\Customers.csv"
    bucket_name = "capstone_proj_9"
    destination_blob_name = "customer.csv"

    upload_to_gcs(local_file_path, bucket_name, destination_blob_name)
