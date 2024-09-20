import asyncio
import json
import logging
import pathlib

import httpx
import pendulum

from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from folioclient import FolioClient


from plugins.inventory.instance import denormalize, folio_id_lookups


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
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def instance_embedding():
    """
    ### Instance Embedding
    This DAG queries for a range of Instances JSON from FOLIO, converts any Reference
    Data with IDs into its text equivalent, and then calls edge-ai to create embeddings of the
    JSON records.
    """

    @task
    def reference_id_lookups():
        return folio_id_lookups(_folio_client())

    @task
    def retrieve_instances():
        context = get_current_context()
        params = context.get("params")
        folio_client = _folio_client()
        temp_location = Variable.get("location", "/opt/airflow/tmp")
        temp_path = pathlib.Path(temp_location)
        temp_path.mkdir(parents=True, exist_ok=True)
        instance_files = []
        file_path = temp_path / f"local-instance-{pendulum.now().isoformat()}.jsonl"
        instance_files.append(str(file_path.absolute()))
        open_file = file_path.open("w+")
        if "uuids" in params:
            for i, uuid in enumerate(params["uuids"]):
                if not i % 250 and i > 0:
                    open_file.close()
                    file_path = (
                        temp_path / f"local-instance-{pendulum.now().isoformat()}.jsonl"
                    )
                    open_file = file_path.open("w+")
                    instance_files.append(str(file_path.absolute()))
                instance = folio_client.folio_get(f"/inventory/instances/{uuid}")
                open_file.write(f"{json.dumps(instance)}\n")

        if "limit" in params:
            limit = params.get("limit", 250)
            for instance in folio_client.folio_get(
                "/inventory/instances",
                key="instances",
                query_params={"limit": limit, "offset": params.get("offset", 0)},
            ):
                open_file.write(f"{json.dumps(instance)}\n")
        open_file.close()
        return instance_files

    @task
    def denormalize_instances(**kwargs):
        denormalize(kwargs["files"], kwargs["references"])

    @task
    def index_instances(**kwargs):
        output = []

        connection = Connection.get_connection_from_secrets("edge_ai")
        host_root = Variable.get("host_root")

        for file_path in kwargs["files"]:
            file_path = file_path.replace("/opt/airflow/", host_root)
            post_result = httpx.post(
                f"{connection.schema}://{connection.host}:{connection.port}/inventory/instance/index",
                json={"source": file_path},
            )
            post_result.raise_for_status()
            logger.info(
                f"Successfully indexed {file_path} details: {post_result.status_code}"
            )
            output.append((file_path, post_result.status_code))
        logger.info(f"Finished Indexing")
        return output

    ref_id_lookups = reference_id_lookups()
    instance_files = retrieve_instances()

    denormalize_instances(
        files=instance_files, references=ref_id_lookups
    ) >> index_instances(files=instance_files)


instance_embedding()
