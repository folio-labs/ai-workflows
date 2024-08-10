import logging

import pendulum

from typing import Union

from airflow.models.connection import Connection
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

from folioclient import FolioClient

from folio_ai_workflows.inventory.instance import (
    enhance,
    reference_data,
    reference_lookups,
)

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 5, tz="UTC"),
    catchup=False,
    tags=["inventory"],
    render_template_as_native_obj=True,
)
def instance_generation():
    """
    ### Instance Generation
    This DAG processes an incoming JSON instance, typically created by a Generative
    AI process. It queries the FOLIO system for reference data required by the
    instance properties. Then, it attempts to match the instance with existing
    instances in FOLIO by calling edge-ai. If a match is found, the existing
    instance is returned; otherwise, the new instance is added to FOLIO.
    """

    @task(multiple_outputs=True)
    def incoming_instance_record() -> dict:
        """
        #### Incoming Instance Record Task
        DAG is triggered with two parameters, `instance` and `jobId` either by edge-ai
        Airflow REST call or directly through the Airflow UI. The `instance` parameters
        is the FOLIO Instance JSON record and the `jobId` is an UUID that will be passed
        back to edge-ai for notification of what the result of running the DAG.
        """
        context = get_current_context()
        params = context.get("params")
        return {"trial_instance": params["instance"], "jobId": params["jobId"]}

    @task(multiple_outputs=True)
    def retrieve_instance_reference_data() -> dict:
        connection = Connection.get_connection_from_secrets("folio")
        folio_client = FolioClient(
            connection.host,
            connection.extra_dejson["tenant"],
            connection.login,
            connection.password,
        )
        return reference_data(folio_client=folio_client)

    @task()
    def enhance_instance(reference_lookups: dict, instance: dict, reference_data: dict):
        return enhance(instance, reference_lookups, reference_data)

    @task.branch
    def match_existing_instances(modified_instance):
        logger.info(
            f"Submit {modified_instance} to edge-ai Instance similarity measure"
        )
        import random

        choice = random.random()
        logger.info(f"Choice is {choice}")
        if choice > 0.7:
            return ["post_instance_to_folio"]
        return ["send_matched_instance"]

    @task()
    def post_instance_to_folio(instance: dict):
        logger.info(f"Would post {instance} to Okapi")
        return True

    @task()
    def send_matched_instance():
        logger.info(f"Would return matched instance (or just matched uuid)")
        return True

    @task(trigger_rule="none_failed_min_one_success")
    def notify_edge_ai(job_id: str):
        logger.info(f"Sends notification to edge-ai with jobId {job_id}")

    setup = incoming_instance_record()

    instance_reference_data = retrieve_instance_reference_data()

    modified_instance = enhance_instance(
        reference_lookups=reference_lookups,
        instance=setup["trial_instance"],
        reference_data=instance_reference_data,
    )

    found_match = match_existing_instances(modified_instance)

    (
        found_match
        >> [send_matched_instance(), post_instance_to_folio(modified_instance)]
        >> notify_edge_ai(setup["jobId"])
    )


instance_generation()
