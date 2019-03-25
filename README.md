# Introduction

This simple application implements an ingestion process to: 
- retrieve the documents from a specified ElasticSearch source,
- send the selected content from these documents to NLP REST service receiving back the annotations from the text,
- send the annotations with selected metadata to specified ElasticSearch sink.

The ingestion parameters (source, sink, fields mapping, etc.) can be set in `congif.yaml` file.


# Usage

Firstly install the Python libraries specified in `requirements.txt`.

To run:
`python main.py --config config.yaml`


# Configuration
The ingestion process properties are configured in `config.yaml`.

## Available options:

### Data Source
Entries under `source` key specify the ElasticSearch source.

### Data sink
Entries under `sink` key specify the ElasticSearch sink.

### Fields mapping
Entries under `mapping` key define the mapping of the document fields for the ingestion.

The sub-entry `source` defines the field names that contain:
- `text-field` - the free text to be processed, 
- `docid-field` - the unique identifier of the document,
- `persis-fields` - a list of fields to be send back to the sink alongside each annotation entry.

The sub-entry `batch` defines the possible portion of documents to be processed according to the date. The used fields are:
- `date-field` - the name of the field containing the date,
- `date-format` - the format of the date/time used to specify the time window by the user (below)
- `date-start` and `date-end` - the time window to be processed.

The sub-entry `sink` specifies additional options during sending the processed annotations:
- `split-index-by-field` - the name of the field in the returned annotations the value of which will be used as a prefix for the index name (e.g., used to send annotations of different types to separate indices).


# Missing
- tests
- multi-node support
- multi-threaded processing
