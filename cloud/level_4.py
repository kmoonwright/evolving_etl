import requests
from random import sample
from datetime import datetime, timedelta

from prefect import task, Flow, Parameter, case
from prefect.tasks.control_flow import ifelse, switch
from prefect.engine.results import LocalResult, S3Result, GCSResult, AzureResult
from prefect.triggers import all_successful, any_failed, some_successful

# Utility Functions
# State handler for a task
def post_to_slack(task, old_state, new_state):
    if new_state.is_finished():
        msg = f"Task {task} finished in state {new_state}"
        requests.post("https:SLACK_WEBHOOK", json={"text": msg})
    return new_state

# State handler for the flow
def my_state_handler(obj, old_state, new_state):
    msg = f"\nCalling my custom state handler on {obj}:\n{old_state} to {new_state}\n"
    print(msg)
    return new_state

# Configurable value taken at runtime
length = Parameter(name="length", default=5, required=False)

# ETL Pipeline Tasks
@task(result=LocalResult(), target="{date:%A}/{task_name}.prefect", state_handlers=[post_to_slack])
def extract(length):
    # Extract the data
    return sample(range(100), length)

@task(max_retries=3, retry_delay=timedelta(seconds=5))
def transform(data):
    # Transform the data
    return data * 10

@task(trigger=some_successful(at_least=1, at_most=6), state_handlers=[post_to_slack])
def load(data):
    # Load the data
    print(f"\nHere's your data: {data}")

# Parameterized Scheduling
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock, CronClock, DatesClock

clock1, clock2 = IntervalClock(
    start_date=datetime.utcnow() + timedelta(seconds=10), 
    interval=timedelta(hours=12), 
    parameter_defaults={"length": 15}
), IntervalClock(
    start_date=datetime.utcnow() + timedelta(seconds=10), 
    interval=timedelta(hours=24), 
    parameter_defaults={"length": 20}
)

schedule = Schedule(clocks=[clock1, clock2])

# Define Tasks in a Flow Context
with Flow('Evolving ETL', 
    result=S3Result(bucket="flow-result-storage"),
    state_handlers=[my_state_handler],
    schedule=schedule
) as flow:
    with case(length, 5):
        e = extract(length)
    with case(length, 50):
        e = extract(length)

    t = transform.map(e)
    l = load(t)

flow.run(parameters={'length': 50}) # Prints data