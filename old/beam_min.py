import apache_beam as beam

def run():
    with beam.Pipeline() as p:
        (p | beam.Create([1, 2, 3])
           | beam.Map(lambda x: print(x)))

if __name__ == '__main__':
    run()
