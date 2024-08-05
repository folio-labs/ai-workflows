# AI Workflows Airflow Demonstration
This [Airflow](https://apache.airflow.org/) environment is used in conjunction 
with the FOLIO [edge-ai](https://github.com/folio-labs/edge-ai) to provide
Generative AI and other machine learning workflows for [FOLIO](https://folio.org/) 
Library Services Platform.


## Example DAGs (Directed Acyclic Graphs)

### Instance Generation
This DAG processes an incoming JSON instance, typically created by a Generative 
AI process. It queries the FOLIO system for reference data required by the 
instance properties. Then, it attempts to match the instance with existing 
instances in FOLIO by calling edge-ai. If a match is found, the existing 
instance is returned; otherwise, the new instance is added to FOLIO.

 
