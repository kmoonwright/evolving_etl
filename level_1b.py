from random import randrange

from prefect import Task, Flow

# ETL Pipeline Tasks
class Extract(Task):
    def run(self):
        # Extract the data
        return randrange(1, 10)

class Transform(Task):
    def run(self, data):
        # Transform the data
        return data * 10

class Load(Task):
    def run(self, data):
        # Load the data
        print(f"\nHere's your data: {data}")

# Define Tasks in a Flow Context
e = Extract()
t = Transform()
l = Load()
flow = Flow('Evolving ETL')

# Set dependency graph
flow.set_dependencies(t, keyword_tasks={'data': e})
flow.set_dependencies(l, keyword_tasks={'data': t})

flow.run() # Prints the data