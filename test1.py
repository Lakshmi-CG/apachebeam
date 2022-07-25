from __future__ import absolute_import
import apache_beam as beam
import logging
import os
import time
import unittest

import pytest

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
p1=beam.Pipeline()

def parse(record: str):
  split_record = record.split(',')
  split_record[8] = 0 if split_record[8] == '-' else split_record[8]
  split_record[9] = 0 if split_record[9] == '-' else split_record[9]
  return(split_record)
  
test1=(
p1
|beam.io.ReadFromText('gs://dfp-bq/test1/test1.txt',skip_header_lines=1)
|beam.Map(lambda record: parse(record))
|beam.io.WriteToText('gs://dfp-bq/results/output'))

p1.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()