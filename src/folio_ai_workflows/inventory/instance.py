import logging

from folioclient import FolioClient
from jsonpath_ng import jsonpath, parse

logger = logging.getLogger(__name__)

reference_lookups = {
    'contributors[*].contributorTypeText': 
       ["Actor",
        "Author",
        "Contributor",
        "Editor",
        "Narrator",
        "Publisher"],
    'contributors[*].contributorNameTypeText': ["Personal name", "Corporate name"],
    'identifiers[*].identifierTypeText': [
       "DOI",
       "ISBN",
       "LCCN",
       "ISSN",
       "OCLC",
       "Local identifier"],
    'instanceTypeText': [
        "text",
        "still image",
        "computer program",
        "computer dataset",
        "two-dimensional moving image",
        "notated music",
        "unspecified",
    ]
}


def reference_data(
    reference_lookups: dict = reference_lookups,
    folio_client: FolioClient) -> dict:
    """Retrieves specific reference data UUIDs from FOLIO"""
    reference_uuids: dict = dict()

    for key, values in reference_lookups.items():
        identifier = key.split(".")[-1].replace("Text", "Id")
        reference_uuids[identifier] = {}
        match identifier:

            case "contributorNameTypeId":
                  contributor_name_types = folio_client.folio_get("/contributor-name-types?limit=500")
                  for row in contributor_name_types["contributorNameTypes"]:
                      if row["name"] in values:
                          reference_uuids[identifier][row["name"]] = row["id"]  

            case "contributorTypeId":
                  contributor_types = folio_client.folio_get("/contributor-types?limit=500")
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
    

