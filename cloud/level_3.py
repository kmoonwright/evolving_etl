from random import sample
from datetime import timedelta

from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import ifelse, switch
from prefect.engine.results import LocalResult, S3Result, GCSResult, AzureResult
from prefect.triggers import all_successful, any_failed, some_successful

# Configurable value taken at runtime
length = Parameter(name="length", default=5, required=False)

# ETL Pipeline Tasks
@task(result=LocalResult(), target="{date:%A}/{task_name}.prefect")
def extract(length):
    # Extract the data
    return sample(range(100), length)

@task(max_retries=3, retry_delay=timedelta(seconds=5))
def transform(data):
    # Transform the data
    return data * 10

@task(trigger=some_successful(at_least=1, at_most=6))
def load(data):
    # Load the data
    print(f"\nHere's your data: {data}")

# Define Tasks in a Flow Context
with Flow('Evolving ETL', result=S3Result(bucket="flow-result-storage")) as flow:
    with case(length, 5):
        e = extract(length)
    with case(length, 50):
        e = extract(length)

    t = transform.map(e)
    l = load(t)

flow.run(parameters={'length': 50}) # Prints data