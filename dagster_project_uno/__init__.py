"""
dagster_project_uno
"""

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    load_assets_from_modules,
    EnvVar,
)

import dagster_dbt
from . import assets
from .resources import DataGeneratorResource

all_assets = load_assets_from_modules([assets])


# Addition: define a job that will materialize the assets
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of
# how frequently to run it
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *",  # every hour
)

# ...

datagen = DataGeneratorResource(
                num_days=EnvVar.int("HACKERNEWS_NUM_DAYS_WINDOW")
                )  # Make the resource

defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule],
    resources={
        "hackernews_api": datagen,  # Add the newly-made resource here
        "dbt": dagster_dbt.dbt_cli_resource
    },
)
