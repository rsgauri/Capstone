import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows
from datetime import timedelta
import logging

# Set your Pub/Sub subscription path
subscription = "projects/orbital-anchor-419817/subscriptions/pubsub_capstone-sub"

class Split(beam.DoFn):
    def process(self, element):
        # Parse and process each element as needed
        # Ensure that each element is a key-value pair
        # Replace this with your data processing logic
        return [(element, element.timestamp)]  # Assuming element is the key and timestamp is attached to each Pub/Sub message

def main():
    PROJECT = 'orbital-anchor-419817'
    schema = 'customer_id:INTEGER, region:INTEGER,channel:INTEGER'

    p = beam.Pipeline(options=PipelineOptions())

    (p
        | 'ReadFromPubSub' >> ReadFromPubSub(subscription=subscription)
        | 'PrintPubSubMessage' >> beam.Map(print)  # Print the Pub/Sub message and its associated timestamp
        | 'WindowInto' >> beam.WindowInto(FixedWindows(size=int(timedelta(minutes=1).total_seconds())))

        # If your data is already in key-value format, you can skip the Split transform
        | 'SplitData' >> beam.ParDo(Split())

        | 'GroupByKey' >> beam.GroupByKey()
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            '{0}:userlogs.logdata'.format(PROJECT),
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)  # Set the logging level
    main()
