from random import randrange
from prefect import Task, Flow

class Extract(Task):
    def run(self):
        return randrange(1, 100)

class Transform(Task):
    def run(self, data):
        return data * 10

class Load(Task):
    def run(self, data):
        print(f"\nHere's your data: {data}")

e = Extract()
t = Transform()
l = Load()

flow = Flow("Evolving ETL")

flow.set_dependencies(t, keyword_tasks={'data': e})
flow.set_dependencies(l, keyword_tasks={'data': t})

flow.run()