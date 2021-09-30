# Introduction

This simple application implements an ingestion process to: 
- retrieve the documents from a specified ElasticSearch source,
- send the selected content from these documents to NLP REST service receiving back the annotations from the text,
- send the annotations with selected metadata to specified ElasticSearch sink.

The ingestion parameters (source, sink, fields mapping, etc.) can be set in `config.yml` file (in `config` directory).


# Usage

Firstly install the Python libraries specified in `requirements.txt`.

To run:
`python main.py --config config/config.yml`

# Configuration
The ingestion process properties are configured in `config.yml`.

## Available options:

### ElasticSearch data store

#### Data Source
Entries under `source` key specify the ElasticSearch source.

#### Data sink
Entries under `sink` key specify the ElasticSearch sink.

#### Security and SSL certificates
This only applies when ElasticSearch cluster is using X-Pack / Open Distro and requires secure connections with using SSL certificates. Under the key `source` and/or `sink` can be optionally specified `security` entry that will contain necessary configuration -- these are:
' `ca-file-path` -- path to CA certificate file (PEM) used solely with SSL, (DO NOT USE THIS along with the other ca/client parameters mentioned underneath, use it solely)
- `ca-certs-path` -- the path to CA certificates file (PEM),
- `client-cert-path` -- the path to client certificate file (PEM),
- `client-key-path` -- the path to client key (PEM).

### Extra ES sink/source connection options
Entires under the key `extra_params` are optional, useful for test cases or deployments where we only use internal resources:
- `use_ssl` -- use ssl connection
- `verify_certs` -- verify SSL certificates 

- `credentials`
    - `username` and `password` can be used to provide connection credentials
    - `use-api-key` if this is enabled the username and password fields will be used as api_id and api_key 

### NLP service
- `endpoint-url` 
- `endpoint-request-mode` , this is either left empty, or in case of use with the GATE NLP Annie annotation service it should be set to `gate-nlp`
- `use-bulk-indexing` ingest in bulk mode (1000 docs / bulk chunk), 

- `credentials`
    - `username` and `password` can be used to provide connection credentials

### Fields mapping
Entries under `mapping` key define the mapping of the document fields for the ingestion.

The sub-entry `index-ingest-mode` defines: 
- `same-index`: `False` , set to `True` if you wish to ingest annotations into the same index
- `use-nested-objects` : 'False` , set to True if you wish to ingest into the same index with nested object type, useful but beware of search query speed impact
- `es-nested-object-schema-mapping` : for medcat annotations use `medcat-separate-index` or `medcat-nested-object` , for GATE-nlp use `gate-nlp-separate-index` or `gate-nlp-nested-object`

The sub-entry `source` defines the field names that contain:
- `text-field` - the free text to be processed, 
- `docid-field` - the unique identifier of the document,
- `persis-fields` - a list of fields to be send back to the sink alongside each annotation entry.

The sub-entry `batch` defines the possible portion of documents to be processed according to the date. The used fields are:
- `date-field` - the name of the field containing the date,
- `date-format` - the format of the date/time used by ElasticSearch to specify the time window by the user (below),
- `python-date-format` - the format of the date/time used by Python to specify the time window by the user (below),
- `interval` - the number of days to be used for incremental batch processing in processing time window,
- `date-start` and `date-end` - the time window to be processed,
- `threads` - the number of processing threads to speed up the ingestion.

The sub-entry `sink` specifies additional options during sending the processed annotations:
- `split-index-by-field` - the name of the field in the returned annotations the value of which will be used as a prefix for the index name (e.g., used to send annotations of different types to separate indices). If you don't want this functionality simply leave the field empty, otherwise , to split by annotation type use `type`

The sub-entry `nlp` specifies additional options during processing the documents with NLP:
- `skip-processed-doc-check` - whether to skip checking for already processed documents in ElasticSearch,
-  `annotation-id-field` - the name of field containing the annotation id returned from the NLP app.

# Missing
- tests
- API specs
