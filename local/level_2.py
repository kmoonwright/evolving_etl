from datetime import datetime, timedelta
from random import sample
from prefect import task, Flow, Parameter, case

length = Parameter(name="length", default=3)

@task
def extract(length):
    return sample(range(100), length)

@task
def transform(data):
    return data * 10

@task
def load(data):
    print(f"\nHere's your data: {data}")

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

clock1 = IntervalClock(
    start_date=datetime.now() + timedelta(seconds=5),
    interval=timedelta(hours=1),
    parameter_defaults={"length": 6}
)
clock2 = IntervalClock(
    start_date=datetime.now() + timedelta(seconds=15),
    interval=timedelta(hours=1),
    parameter_defaults={"length": 50}
)

schedule = Schedule(clocks=[clock1, clock2])

with Flow("Evolving ETL", schedule=schedule) as flow:
    with case(length, 6):
        e = extract(length)
        t = transform.map(e)
        l = load(t)

    with case(length, 50):
        e = extract(length)
        t = transform.map(e)
        t2 = transform.map(t)
        l = load(t2)

flow.run()