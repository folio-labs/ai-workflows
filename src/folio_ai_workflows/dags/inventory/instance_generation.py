import logging

import pendulum
from jsonpath_ng import jsonpath, parse

from typing import Union

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context

from folio_ai_workflows.inventory.instance import (
    enhance,
    reference_data
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
        return {
            "trial_instance": params['instance'],
            "jobId": params['jobId']
        }


    @task(multiple_outputs=True)
    def retrieve_instance_reference_data() -> dict:
        folio_client = FolioClient(
            Variable.get("okapi_url"),
            Variable.get("folio_tenant"),
            Variable.get("folio_user"),
            Variable.get("folio_user_password"),
        ) 
        return reference_data(folio_client=folio_client)


    @task()
    def enhance_instance(reference_lookups: dict, instance: dict, reference_data: dict):
        for name in reference_lookups.keys():
            text_key = name.split(".")[-1]
            ref_key = text_key.replace("Text", "Id")
            path_expression = parse(name)
            for match in path_expression.find(instance):
                if match.value not in reference_data[ref_key]:
                    logger.error(f"{value} not found in reference data's {ref_key}")
                    continue
                parent = match.context.value
                parent[ref_key] = reference_data[ref_key][match.value]
                del parent[text_key]
                logger.info(f"Replaced {text_key} value {match.value} with {ref_key} UUID")
        return instance 


    @task()
    def match_existing_instances(modified_instance):
         logger.info(f"Submit {modified_instance} to edge-ai Instance similarity measure")
         import random
         if random.random() > .5:
             return True
         return False

    @task()
    def post_instance_to_folio(instance: dict):
        logger.info(f"Would post {instance} to Okapi")
        return True


    setup = incoming_instance_record()

    reference_data = retrieve_instance_reference_data()

    modified_instance = enhance_instance(reference_lookups=reference_lookups, instance=setup['trial_instance'], reference_data=reference_data)

    found_match = match_existing_instances(modified_instance)

    post_result = post_instance_to_folio(modified_instance)
        
    
    
instance_generation()
