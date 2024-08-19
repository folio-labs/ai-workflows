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


def _expand_contributors(instance: dict, ref_data_lookups: dict):
    for contributor in instances.get("contributors", []):
        contributor_name_type = contributor.pop("contributorNameTypeId")
        contributor["contributorNameTypeText"] = ref_data_lookups["contributorNameTypeId"].get(contributor_name_type, "Unknown")
        contributor_type = contributor.pop("contributorTypeId")
        contributor["contributorTypeText"] = ref_data_lookups["contributorTypeId"].get(contributor_type, "Unknown")


def _expand_identifiers(instance: dict, ref_data_lookups: dict):
    for identifier in instance.get("identifiers", []):
        identifier_type = identifier.pop("identifierTypeId")
        identifier["identifierTypeText"] = ref_data_lookups["identifierTypeId"].get(identifier_type, "Unknown")


def _expand_references(instances: list, ref_data_lookups: dict) -> list:
    for i, instance in enumerate(instances):
        # Remove metadata
        instance.pop("metadata")

        _expand_contributors(instance, ref_data_lookups)
        _expand_identifiers(instance, ref_data_lookups)
        
        instance_type_id = instance.pop("instanceTypeId")
        instance["instanceTypeText"] = ref_data_lookups["instanceTypeId"].get(instance_type_id, "Unknown")
    return instances


def _reference_data(folio_client: FolioClient) -> dict:
    lookups = {
        "contributorNameTypeId": {},
        "contributorTypeId": {},
        "identifierTypeId": {},
        "instanceTypeId": {},
    }
    contributor_name_types = folio_client.folio_get(
                    "/contributor-name-types?limit=500"
    )
    for row in contributor_name_types["contributorNameTypes"]:
        lookups["contributorNameTypeId"][row["id"]] = row["name"]
    contributor_types = folio_client.folio_get(
        "/contributor-types?limit=500"
    )
    for row in contributor_types["contributorTypes"]:
        lookups["contributorTypeId"][row["id"]] = row["name"]
    identifier_types = folio_client.folio_get("/identifier-types?limit=500")
    for row in identifier_types["identifierTypes"]:
        lookups["identifierTypeId"][row["id"]] = row["name"]
    instance_types = folio_client.folio_get("/instance-types?limit=500")
    for row in instance_types["instanceTypes"]:
        lookups["instanceTypeId"][row["id"]] = row["name"]
    return lookups


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


def denormalize(instance_files: list, folio_client: FolioClient):
    references = _reference_data(folio_client)
    for row in instance_files:
        file_path = pathlib.Path(row)
        with file_path.open() as fo:
            records = json.load(fo)
        records = _expand_references(records, references)
        with file_path.open("w+") as fo:
            json.dump(records, fo)
        logger.info(f"Finished modifying {len(records):,} in {file_path}")


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
