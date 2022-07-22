from apache_beam.transforms.core import Map

import apache_beam as beam
p1=beam.Pipeline()

def parse(record: str):
  split_record = record.split(',')
  split_record[8] = 0 if split_record[8] == '-' else split_record[8]
  split_record[9] = 0 if split_record[9] == '-' else split_record[9]
  return(split_record)
  
test1=(
p1
|beam.io.ReadFromText('test1.csv',skip_header_lines=1)
|beam.Map(lambda record: parse(record))
|beam.io.WriteToText('replace1.csv'))
p1.run()