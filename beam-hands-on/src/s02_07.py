import apache_beam as beam

p = beam.Pipeline()

count = (
    p
    | "Read file" >> beam.io.ReadFromText("dept_data.txt")
    | "Split row" >> beam.Map(lambda record: record.split(","))
    | "Filter accounts" >> beam.Filter(lambda record: record[3] == "Accounts")
    | "Turn to tuple" >> beam.Map(lambda record: (record[1], 1))
    | "Count by key" >> beam.CombinePerKey(sum)
    | "Get employee counts" >> beam.Map(lambda employee_count: str(employee_count))
)


write = count | "Write to file" >> beam.io.WriteToText("data_src/output")

p.run()
