
import apache_beam as beam

pipeline1 = beam.Pipeline()

airline_count = (
 pipeline1
 |beam.io.ReadFromText('/home/shrutijoshi/Flights_Data/archive/flights.csv')
 |beam.Map(lambda line: line.split(','))
 |beam.Filter(lambda line: line[0] == '2015')
 |beam.Map(lambda line: (line[4], 1))
 |beam.CombinePerKey(sum)
 |beam.io.WriteToText('/home/shrutijoshi/output_data')
 )

pipeline1.run()