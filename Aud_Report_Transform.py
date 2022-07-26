from apache_beam.transforms.core import Map

import apache_beam as beam
p1=beam.Pipeline()

def parse(record: str):
  split_record = record.split(',')
  split_record[27] = 0 if split_record[27] == '-' else split_record[27]
  split_record[28] = 0 if split_record[28] == '-' else split_record[28]
  split_record[29] = 0 if split_record[29] == '-' else split_record[29]
  split_record[30] = 0 if split_record[30] == '-' else split_record[30]
  split_record[31] = 0 if split_record[31] == '-' else split_record[31]

 
  return(split_record)
  
test1=(
p1
|beam.io.ReadFromText('gs://dfp-bq/C:\Reports/AUD_OrderID-2871434381.csv',skip_header_lines=1)

|beam.Map(lambda record: parse(record))
|beam.io.WriteToText('gs://dfp-bq/ProcessedFiles/output'))
p1.run().wait_until_finish()
