import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

class DataflowOptions(PipelineOptions):
    """Custom command-line options for the Dataflow pipeline."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input', required=True, help='GCS path to the input file')
        parser.add_argument('--output', required=True, help='BigQuery table to write data')

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa_key.json"

# Initialize pipeline options
pipeline_options = DataflowOptions()

google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = "my-first-pipeline-453507"
google_cloud_options.temp_location = "gs://pipeline-bucket-003/temp/"

# Set Dataflow runner
pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

# Define BigQuery schema
BQ_SCHEMA = 'order_id:INTEGER, product:STRING, quantity:INTEGER, order_status:STRING, order_date:DATE'

def parse_csv_to_dict(line):
    """Parses a CSV line into a dictionary and filters only 'Completed' orders."""
    columns = line.strip().split(',')
    if len(columns) != 5:
        return None  # Skip invalid rows
    
    try:
        order = {
            "order_id": int(columns[0]),
            "product": columns[1],
            "quantity": int(columns[2]),
            "order_status": columns[3],
            "order_date": columns[4]
        }
        return order if order["order_status"] == "Completed" else None
    except ValueError:
        return None  # Skip rows with conversion errors

# Define pipeline
def run():
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'ReadFromGCS' >> beam.io.ReadFromText(pipeline_options.input, skip_header_lines=1)
            | 'ParseToDict' >> beam.Map(parse_csv_to_dict)
            | 'FilterValidRows' >> beam.Filter(lambda x: x is not None)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                pipeline_options.output,
                schema=BQ_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()

