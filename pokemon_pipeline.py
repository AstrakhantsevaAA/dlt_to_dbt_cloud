"""Very simple pokemon pipeline, to be used as a starting point for new pipelines.

Available resources:
    fruits
    vegetables
"""
import dlt
from pokemon import source
from typing import List


def load(resources: List[str]) -> None:
    """
    Execute a pipeline that will load all the resources for the given endpoints.
    Args:
        resources (List[str]): A list of resource names to load data from the pokemon source. Available resources include 'pokemon' and 'berries'.
    Returns:
        None: This function doesn't return any value. It prints the loading information on successful execution.
    """
    pipeline = dlt.pipeline(
        pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data"
    )

    load_info = pipeline.run(source().with_resources(*resources))
    print(load_info)


# # Create a transformation on a new dataset called 'pipedrive_dbt'
# # we created a local dbt package
# # and added pipedrive_raw to its sources.yml
# # the destination for the transformation is passed in the pipeline
# pipeline = dlt.pipeline(
#     pipeline_name="pokemon", destination="duckdb", dataset_name="pokemon_data_dbt"
# )

# # make or restore venv for dbt, using latest dbt version
# venv = dlt.dbt.get_venv(pipeline)

# # get runner, optionally pass the venv
# dbt = dlt.dbt.package(
#     pipeline,
#     "pokemon/dbt_pokemon/pokemon/models/example/my_second_dbt_model.sql",
#     venv=venv,
# )

# # run the models and collect any info
# # If running fails, the error will be raised with full stack trace
# models = dbt.run_all()

# # on success print outcome
# for m in models:
#     print(
#         f"Model {m.model_name} materialized"
#         + f"in {m.time}"
#         + f"with status {m.status}"
#         + f"and message {m.message}"
#     )


# if __name__ == "__main__":
#     """
#     Main function to execute the data loading pipeline.
#     Add your desired resources to the list and call the load function.
#     """
#     resources = ["pokemon", "berries"]
#     load(resources)
