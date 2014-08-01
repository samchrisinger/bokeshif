from invoke import task

from scripts.analyze import stats as get_stats
from scripts.seed_elastic import seed as seed_ES

@task
def seed_elastic(size=100000, schema='basic'):
    seed_ES(size, schema)

@task
def stats():
    get_stats()
