"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
from typing import List

import dlt

from dbt_cloud_function import run_dbt_cloud_job
from pokemon import source


def load(resources: List[str]) -> None:
    """
    Execute a pipeline that will load all the resources for the given endpoints.
    Args:
        resources (List[str]): A list of resource names to load data from the pokemon source. Available resources include 'pokemon' and 'berries'.
    Returns:
        None: This function doesn't return any value. It prints the loading information on successful execution.
    """
    pipeline = dlt.pipeline(
        pipeline_name="pokemon", destination="bigquery", dataset_name="pokemon_data"
    )

    load_info = pipeline.run(source().with_resources(*resources))
    print(load_info)

    # Trigger job run and wait for an outcome
    run_status = run_dbt_cloud_job(wait_for_outcome=True)
    print(run_status)

    # Trigger job run without waiting for outcome
    # run_id = run_dbt_cloud_job(wait_for_outcome=False)


if __name__ == "__main__":
    """
    Main function to execute the data loading pipeline.
    Add your desired resources to the list and call the load function.
    """
    resources = ["pokemon", "berries"]
    load(resources)
