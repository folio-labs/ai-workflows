import ast
import json
import logging
import pathlib

import httpx

from typing import Union

from airflow.models.connection import Connection
from folioclient import FolioClient
from jsonpath_ng import parse

logger = logging.getLogger(__name__)


def match_instance(instance: dict, cutoff: int = 80) -> Union[str, None]:
    connection = Connection.get_connection_from_secrets("edge_ai")
    match_result = httpx.post(
        f"{connection.schema}://{connection.host}:{connection.port}/inventory/instance/similarity",
        json={
            "text": instance,
        },
        timeout=60,
    )

    match_result.raise_for_status()
    match_payload = match_result.json()
    logger.info(f"Match payload {match_payload}")
    try:
        matches = ast.literal_eval(match_payload["score"])
    except SyntaxError as e:
        logger.error(f"Syntax error trying to parse {match_payload["score"]}")
        matches = {}
    for uuid, score in matches.items():
        if int(score) >= cutoff:
            return uuid
    logger.info(f"Matches {match_payload['score']} are below {cutoff} cutoff")

