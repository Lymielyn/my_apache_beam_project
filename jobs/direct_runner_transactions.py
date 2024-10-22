"""
Python script to create a simple Apache Beam pipeline to extract sample transactions data set from BigQuery. Transformed data to be saved in output directory

This version uses composite transformation
"""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from datetime import datetime
import json
import logging
import os

def convert_datetime_to_date_object(x):
   """Convert the datetime string into a datetime object."""
   date_object = datetime.strptime(x, '%Y-%m-%d %H:%M:%S %Z')
   return date_object

def format_date(x):
   """Extract and format the date from a datetime string."""
   date_object = convert_datetime_to_date_object(x)
   form_date = date_object.date().isoformat()
   return form_date

def to_jsonl(x):
    """Convert each tuple to a JSON string."""
    date, total_amount = x
    return json.dumps({"date": date, "total_amount": round(total_amount, 2)})  # have to round up to two decimal points otherwise test will fail

# Output file being generated with 0000-of-0001 (shard number)
def rename_output_file(output_name):
    """This function will check for any file containing the shard number - "0000-of-00001" and rename it"""
    shard_file = f'{output_name}-00000-of-00001{output_suffix}'
    final_output_file = output_name+output_suffix

    # Check if the sharded file exists
    if os.path.exists(shard_file):
        os.rename(shard_file, final_output_file)
        print(f'Renamed {shard_file} to {final_output_file}')
    else:
        print(f'{shard_file} does not exist')

class CompTransform(beam.PTransform):
   def expand(self, input_):
      transf_steps = (
         input_ 
          # TODO: Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
              | "Split lines" >> beam.Map(lambda x: x.split(","))

              # TODO: Find all transactions have a `transaction_amount` greater than `20`
              | "Filter out transactions less than 20" >> beam.Filter(lambda x: float(x[3]) > 20)

              # TODO: Exclude all transactions made before the year `2010`
              | "Filter out transactions made before 2010" >> beam.Filter(lambda x: convert_datetime_to_date_object(x[0]).year > 2010)
              
              # TODO: Sum the total by `date`
              | "Exclude other columns apart from date and transaction_amount" >> beam.Map(lambda x: (format_date(x[0]) , float(x[3])))
              # Not necessary for the sample data but still included in the pipeline
              | "Group by date" >> beam.GroupByKey()
              | "Sum transactions by dates" >> beam.Map(lambda kv: (kv[0], sum(kv[1]))) 
              # | "Print" >> beam.Map(print)
              # TODO: Save the output into `output/results.jsonl.gz`
              | "Format to JSONL" >> beam.Map(to_jsonl)  # Convert to JSON Lines format
      )
      return transf_steps

# Configure pipeline options
input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_file = 'output/results'
output_suffix = '.jsonl.gz'

pipeline_options = PipelineOptions(
  # Set pipeline runner to be DirectRunner (optional)
  runner = 'DirectRunner'
)

# Building my pipeline here
def run_pipeline(argv=None):
   # Define arguments
   parser = argparse.ArgumentParser()

   parser.add_argument(
      '--source_path',
      dest='source_path',
      default=input_file,
      help='Input file to read from')
   parser.add_argument(
      '--target_path',
      dest='target_path',
      default=output_file,
      help='Output file to write results to')
   parser.add_argument(
      '--file_suffix',
      dest='file_suffix',
      default='.jsonl.gz',
      help='File extension value')
   
   known_args, pipeline_args = parser.parse_known_args(argv)

   # Build pipeline here
   with beam.Pipeline(options=pipeline_options) as pipeline:
     # Transformation inside the pipeline
     (pipeline            
              # TODO: Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
              | "Reading CSV" >> ReadFromText(known_args.source_path, skip_header_lines=True)
              | "Using composite transformation" >> CompTransform()
              | "Write to GZipped JSONL" >> WriteToText(
                known_args.target_path, 
                file_name_suffix=known_args.file_suffix, 
                compression_type=beam.io.filesystems.CompressionTypes.GZIP,
                num_shards=1 # Adding this to avoid shard numbering
                )
     )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline for transactions CSV file")
    run_pipeline() # Pipeline being run

    # After the pipeline is run, rename the file
    rename_output_file(output_file)
