"""
This is data flow job which will be used to create data flow template in order to trigger from cloud function.
"""

import datetime
import apache_beam as beam
import uuid

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
import logging
import argparse

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_path', type=str,
            help='The input GCS location to read data.',
            default='gs://default')
        parser.add_argument(
            '--bigtable_project', type=str,
            help='The Bigtable project ID, this can be different than your '
                 'Dataflow project',
            default='helpful-cat-324414')
        parser.add_argument(
            '--bigtable_instance', type=str,
            help='The Bigtable instance ID',
            default='dezyre')
        parser.add_argument(
            '--bigtable_table', type=str,
            help='The Bigtable table ID in the instance.',
            default='covid')


class CreateRowFn(beam.DoFn):
    def process(self, element):
        from google.cloud.bigtable import row
        import csv
        #print(str(file_path))
        #logging.info(str(file_path))
        rows = []
        headers = ['Case_Type', 'People_Total_Tested_Count', 'Cases', 'Difference', 'Date', 'Combined_Key', 'Country_Region', 'Province_State', 'Admin2', 'iso2', 'iso3', 'FIPS', 'Lat', 'Long', 'Population_Count', 'People_Hospitalized_Cumulative_Count', 'Data_Source', 'Prep_Flow_Runtime']
        #print(headers)
        print(element)
        for i, columnList in enumerate(csv.reader(element.split('\n'), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)):
            #print(i)
            if i>=0:
                columns = columnList
                print(columns)
                key = "covid_cases#"+columns[5]+"#"+str(uuid.uuid4().hex)
                print(key)
                direct_row = row.DirectRow(row_key=key)
                for j, column in enumerate(columns):
                    #print(j)
                    #print(column)
                    direct_row.set_cell(
                        "data",
                        headers[j].encode(),
                        column.encode(),
                        datetime.datetime.now())
                yield direct_row
                #rows.append(direct_row)
        #return rows


def run(argv=None):
    """Build and run the pipeline."""
    options = MyOptions(argv, save_main_session=True)
    with beam.Pipeline(options=options) as p:
        p | beam.io.ReadFromText(options.input_path, skip_header_lines=1) | 'Parse csv ' >>  beam.ParDo(
                CreateRowFn()) | WriteToBigTable(
            project_id=options.bigtable_project,
            instance_id=options.bigtable_instance,
            table_id=options.bigtable_table)



if __name__ == '__main__':
    run()