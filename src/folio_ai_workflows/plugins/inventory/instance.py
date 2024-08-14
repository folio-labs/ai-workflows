import logging

from folioclient import FolioClient
from jsonpath_ng import jsonpath, parse

logger = logging.getLogger(__name__)

reference_lookups = {
    "contributors[*].contributorTypeText": [
        "Actor",
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
}


def enhance(instance: dict, reference_lookups: dict, reference_data: dict) -> dict:
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


def reference_data(
    folio_client: FolioClient,
    reference_lookups: dict = reference_lookups,
) -> dict:
    """Retrieves specific reference data UUIDs from FOLIO"""
    reference_uuids: dict = dict()

    for key, values in reference_lookups.items():
        identifier = key.split(".")[-1].replace("Text", "Id")
        reference_uuids[identifier] = {}
        match identifier:

            case "contributorNameTypeId":
                contributor_name_types = folio_client.folio_get(
                    "/contributor-name-types?limit=500"
                )
                for row in contributor_name_types["contributorNameTypes"]:
                    if row["name"] in values:
                        reference_uuids[identifier][row["name"]] = row["id"]

            case "contributorTypeId":
                contributor_types = folio_client.folio_get(
                    "/contributor-types?limit=500"
                )
                for row in contributor_types["contributorTypes"]:
                    if row["name"] in values:
                        reference_uuids[identifier][row["name"]] = row["id"]

            case "identifierTypeId":
                identifier_types = folio_client.folio_get("/identifier-types?limit=500")
                for row in identifier_types["identifierTypes"]:
                    if row["name"] in values:
                        reference_uuids[identifier][row["name"]] = row["id"]

            case "instanceTypeId":
                instance_types = folio_client.folio_get("/instance-types?limit=500")
                for row in instance_types["instanceTypes"]:
                    if row["name"] in values:
                        reference_uuids[identifier][row["name"]] = row["id"]

            case _:
                logger.error(f"Unknown identifier type {identifier}")

    return reference_uuids
