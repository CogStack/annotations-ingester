# Information

This simple application implements an ingestion process to: 
- retrieve the documents from specified ElasticSearch source,
- send the selected content from these documents to NLP REST service to receive back the annotations from the text,
- send the annotations to specified ElasticSearch sink.

The ingestion process can be configured in `congif.yaml` file.

# Usage

Firstly install the Python libraries specified in `requirements.txt`.

To run:
`python main.py --config config.yaml`


# TODO:
- tests
- provide Dockerfile
- proper handling ElasticSearch bulk requests
