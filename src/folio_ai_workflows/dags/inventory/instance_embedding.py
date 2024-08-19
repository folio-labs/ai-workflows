import logging
import pathlib

import pendulum

from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

from folioclient import FolioClient


from folio_ai_workflows.plugins.inventory.instance import (
    denormalize

)

logger = logging.getLogger(__name__)

def _folio_client() -> FolioClient:
    connection = Connection.get_connection_from_secrets("folio")
    return FolioClient(
        connection.host,
        connection.extra_dejson["tenant"],
        connection.login,
        connection.password,
    )

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 14, tz="UTC"),
    catchup=False,
    tags=["inventory"],
    render_template_as_native_obj=True,
)
def instance_embedding():
    """
    ### Instance Embedding
    This DAG queries for a range of Instances JSON from FOLIO, converts any Reference
    Data with IDs into its text equivalent, and then creates an embedding of the 
    JSON record that is then ingested into a Vector Datastore for use in RAG
    workflows by edge-ai.
    """
    @task
    def reference_id_lookups():

        return {}

    @task
    def retrieve_instances():
        context = get_current_context()
        params = context.get("params")
        folio_client = _folio_client()
        temp_location = Variable.get("location", "/opt/airflow/tmp")
        temp_path = pathlib.Path(temp_location)
        temp_path.mkdir(parents=True, exist_ok=True)
        instance_files = []
        if "uuids" in params and len(params["uuids"]) < 250:
            file_path = temp_path / "local-instance-{pendulum.now().isoformat()}.jsonl"
            with file_path.open("w+") as fo:
                for uuid in params["uuids"]:
                    instance = folio_client.folio_get(f"/inventory/instance/{uuid}")
                    fo.write(f"{json.dumps(instance)}\n")
            instance_files.append(str(file_path.absolute()))
        return instance_files

    @task
    def denormalize_instances(**kwargs):
        denormalize(kwargs["files"], _folio_client())
        

    ref_id_lookups = reference_id_lookups()
    instance_files = retrieve_instances()

    denormalize_instances(reference_lookups=ref_id_lookups, files=instance_files) 
       
            



instance_embedding()
