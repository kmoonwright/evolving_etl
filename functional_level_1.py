from random import randrange

from prefect import task, Flow

# ETL Pipeline Tasks
@task
def extract():
    # Extract the data
    return randrange(1, 100)

@task
def transform(data):
    # Transform the data
    return data * 10

@task
def load(data):
    # Load the data
    print(f"\nHere's your data: {data}")

# Define Tasks in a Flow Context
with Flow('Evolving ETL') as flow:
    e = extract()
    t = transform(e)
    l = load(t)

flow.run() # Prints the data