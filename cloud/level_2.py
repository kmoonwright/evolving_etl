from random import sample
from datetime import timedelta

from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import ifelse, switch

# Configurable value taken at runtime
length = Parameter(name="length", default=5, required=False)

# ETL Pipeline Tasks
@task
def extract(length):
    # Extract the data
    return sample(range(100), length)

@task()
def transform(data):
    # Transform the data
    return data * 10

@task
def load(data):
    # Load the data
    print(f"\nHere's your data: {data}")

# Define Tasks in a Flow Context
with Flow('Evolving ETL') as flow:
    with case(length, 5):
        e = extract(length)
    with case(length, 50):
        e = extract(length)

    t = transform.map(e)
    l = load(t)

flow.run(parameters={'length': 50}) # Prints data