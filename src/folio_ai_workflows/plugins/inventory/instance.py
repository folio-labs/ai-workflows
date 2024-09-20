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

reference_lookups = {
    "classifications[*].classificationTypeText": [
        "Canadian Classification",
        "NLM",
        "SUDOC",
        "LC",
        "LC (local)",
        "National Agricultural Library",
    ],
    "contributors[*].contributorTypeText": [
        "Actor",
        "Artist",
        "Author",
        "Contributor",
        "Editor",
        "Narrator",
        "Publisher",
    ],
    "contributors[*].contributorNameTypeText": ["Personal name", "Corporate name"],
    "identifiers[*].identifierTypeText": [
        "DOI",
        "ISBN",
        "LCCN",
        "ISSN",
        "OCLC",
        "Local identifier",
    ],
    "instanceTypeText": [
        "text",
        "still image",
        "computer program",
        "computer dataset",
        "two-dimensional moving image",
        "notated music",
        "unspecified",
    ],
    "modeOfIssuanceText": [
        "Collection",
        "Monographic component part",
        "multipart monograph",
        "Serial component part",
        "serial",
        "single unit",
        "unspecified",
    ],
}


def _expand_property(**kwargs):
    instance: dict = kwargs["instance"]
    prop_name: str = kwargs["property_name"]
    type_id: str = kwargs["property_type_id"]
    type_lookups: dict = kwargs["type_lookups"]

    type_id_text = type_id.replace("Id", "Text")

    for row in instance.get(prop_name, []):
        if type_id not in row:
            continue
        instance_prop_type = row.pop(type_id)
        row[type_id_text] = type_lookups[type_id].get(instance_prop_type, "Unknown")


def _expand_references(instances: list, ref_data_lookups: dict) -> list:
    for instance in instances:
        # Properties with lists
        for row in [
            ("contributors", "contributorNameTypeId"),
            ("contributors", "contributorTypeId"),
            ("classifications", "classificationTypeId"),
            ("identifiers", "identifierTypeId"),
            ("notes", "instanceNoteTypeId"),
        ]:
            _expand_property(
                instance=instance,
                property_name=row[0],
                property_type_id=row[1],
                type_lookups=ref_data_lookups,
            )
        # Properties with string values
        for row in ["instanceTypeId", "modeOfIssuanceId"]:
            prop_id = instance.pop(row)
            prop_text = row.replace("Id", "Text")
            instance[prop_text] = ref_data_lookups[row].get(prop_id, "Unknown")

    return instances


def _set_defaults(instance: dict):
    for contributor in instance.get("contributors", []):
        if "contributorNameTypeText" not in contributor:
            contributor["contributorNameTypeText"] = "Personal name"


def denormalize(instance_files: list, references: dict):
    for row in instance_files:
        file_path = pathlib.Path(row)
        records = []
        with file_path.open() as fo:
            for line in fo.readlines():
                records.append(json.loads(line))
        logger.info(f"Total of {len(records):,} records")
        records = _expand_references(records, references)
        with file_path.open("w+") as fo:
            for record in records:
                fo.write(f"{json.dumps(record)}\n")
        logger.info(f"Finished modifying {len(records):,} in {file_path}")
    logger.info(records)


def enhance(instance: dict, reference_lookups: dict, reference_data: dict) -> dict:
    _set_defaults(instance)
    for name in reference_lookups.keys():
        text_key = name.split(".")[-1]
        ref_key = text_key.replace("Text", "Id")
        path_expression = parse(name)
        matches = path_expression.find(instance)
        for match in matches:
            if match.value not in reference_data[ref_key]:
                logger.error(f"{match.value} not found in reference data's {ref_key}")
                continue
            parent = match.context.value
            parent[ref_key] = reference_data[ref_key][match.value]
            del parent[text_key]
            logger.info(f"Replaced {text_key} value {match.value} with {ref_key} UUID")
    return instance


def folio_id_lookups(folio_client: FolioClient) -> dict:
    lookups = {}
    for row in [
        ("/classification-types", "classificationTypes", "classificationTypeId"),
        ("/contributor-name-types", "contributorNameTypes", "contributorNameTypeId"),
        ("/contributor-types", "contributorTypes", "contributorTypeId"),
        ("/identifier-types", "identifierTypes", "identifierTypeId"),
        ("/instance-note-types", "instanceNoteTypes", "instanceNoteTypeId"),
        ("/instance-statuses", "instanceStatuses", "statusId"),
        ("/instance-types", "instanceTypes", "instanceTypeId"),
        ("/modes-of-issuance", "issuanceModes", "modeOfIssuanceId"),
    ]:
        folio_result_list = folio_client.folio_get(
            row[0], key=row[1], query_params={"limit": 500}
        )
        lookups[row[2]] = {}
        for result in folio_result_list:
            lookups[row[2]][result["id"]] = result["name"]
    return lookups


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


def reference_data(
    folio_client: FolioClient,
    reference_lookups: dict = reference_lookups,
) -> dict:
    """Retrieves specific reference data UUIDs from FOLIO"""
    reference_uuids: dict = dict()
    lookups = folio_id_lookups(folio_client)

    for key, values in reference_lookups.items():
        identifier = key.split(".")[-1].replace("Text", "Id")
        reference_uuids[identifier] = {}
        lookup_identifiers = lookups[identifier]
        for lookup_key, lookup_values in lookup_identifiers.items():
            for value in values:
                if value in lookup_values:
                    reference_uuids[identifier][value] = lookup_key
                else:
                    logger.error(f"Unknown value: {value} for {identifier}")

    return reference_uuids
