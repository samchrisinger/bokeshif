from invoke import run, task

from scripts.seed_elastic import seed as seed_ES

@task
def seed_elastic(size=100000):
    seed_ES(size)
