# AI Workflows Airflow Demonstration
This [Airflow](https://apache.airflow.org/) environment is used in conjunction 
with the FOLIO [edge-ai][EDGE_AI] to provide
Generative AI and other machine learning workflows for [FOLIO](https://folio.org/) 
Library Services Platform.


### Airflow Connections and Variables

### edge-ai
Run edge-ai locally and create an Airflow HTTP Connection
Follow the directions for running [edge-ai][EDGE_AI] locally on your computer and 
then create a new HTTP Connection `edge-ai` with the following parameters:

- host: `host.docker.internal`
- schema: `http`
- port: 8000

### FOLIO
Create a `folio` connection with the following parameters:
- host: okapi-url
- login: folio user
- password: folio user password
- json: ```{ "tenant": "tenant_code" }``

### Variables
To run the embedding DAGs, you'll need to set the following Airflow variable:
- host_root: Full path to the local root directory of `ai-workflows`

## Example DAGs (Directed Acyclic Graphs)

### Instance Generation
This DAG processes an incoming JSON instance, typically created by a Generative 
AI process. It queries the FOLIO system for reference data required by the 
instance properties. Then, it attempts to match the instance with existing 
instances in FOLIO by calling edge-ai. If a match is found, the existing 
instance is returned; otherwise, the new instance is added to FOLIO.

[EDGE_AI]: https://github.com/folio-labs/edge-ai 
