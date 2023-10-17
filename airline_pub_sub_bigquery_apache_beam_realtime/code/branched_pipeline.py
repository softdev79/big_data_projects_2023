import apache_beam as beam

branched_pipeline = beam.Pipeline()
input_collection = (
 branched_pipeline
 | "Read from text file" >> beam.io('/home/shrutijoshi/Flights_Data/archive/flights.csv')
 | "Split rows" >> beam.Map(lambda line: line.split(',')))

flight_month1 = (input_collection
 | 'Retrieve month1 flights' >> beam.Filter( lambda line: line[1] == '1')
 | 'Pair them 1–1 for month1' >> beam.Map( lambda line: ("Month1, " +line[4], 1))
 | 'Aggregation Operations: Grouping & Summing1' >> beam.CombinePerKey(sum))
flight_month2 = (input_collection
 |'Retrieve month2 flights' >> beam.Filter( lambda line: line[1] == '2')
 |'Pair them 1–1 for month2' >> beam.Map( lambda line: ("Month2, " +line[4], 1))
 | 'Aggregation Operations: Grouping & Summing2' >> beam.CombinePerKey(sum))

output = (
 (flight_month1, flight_month2)
 | beam.Flatten()
 | beam.io.WriteToText('/home/shrutijoshi/branched.txt') )

branched_pipeline.run()

