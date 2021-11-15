#!/usr/bin/python

import argparse
import yaml
import logging

from ingester.es_common import *
from ingester.nlp_service import *
from ingester.annotations_indexer import *

class AppConfig:
    """
    The configuration file for the indexer application
    """
    def __init__(self, file_path):
        """
        :param: filepath: the path for the configuration file stored in YAML
        """
        try:
            with open(file_path) as conf_file:
                yaml_file = yaml.safe_load(conf_file)

                if 'source' not in yaml_file or \
                        'nlp-service' not in yaml_file or \
                        'sink' not in yaml_file or \
                        'mapping' not in yaml_file:
                    raise Exception("Invalid configuration file provided")

                self.params = yaml_file

        except FileNotFoundError:
            raise Exception("Cannot open configuration file")

if __name__ == "__main__":
    # parse the input parameters
    parser = argparse.ArgumentParser(description='ElasticSearch-to-ElasticSearch annotations indexer')
    parser.add_argument('--config', help='configuration file path')
    args = parser.parse_args()

    if args.config is None:
        parser.print_usage()
        exit(0)

    try:
        config = AppConfig(args.config)

        # setup logging
        log_format = '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s'
        if config.params['logging-level']:
            logging.basicConfig(format=log_format, level=int(config.params['logging-level']))
        else:
            logging.basicConfig(format=log_format, level=logging.INFO)
        logging.getLogger('elasticsearch')

        # initialize the elastic source
        source_params = config.params['source']

        source_credentials = source_params['es']['credentials'] if source_params['es']['credentials'] else None
        source_extra_params = source_params['es']['extra-params'] if source_params['es']['extra-params'] else None
        source_security = source_params['es']['security']  if source_params['es']['security'] else None

        source_ssl_config = SslConnectionConfig(ca_file_path=source_security['ca-file-path'],
                                                ca_certs_path=source_security['ca-certs-path'],
                                                client_cert_path=source_security['client-cert-path'],
                                                client_key_path=source_security['client-key-path'])

        es_source_conf = ElasticConnectorConfig(hosts=source_params['es']['hosts'], credentials=source_credentials, extra_params=source_extra_params,
                                                   ssl_config=source_ssl_config)

        es_source_conn = ElasticConnector(es_source_conf)
        es_source = ElasticRangedIndexer(es_source_conn, source_params['es']['index-name'])

        # initialize NLP service
        nlp_service = NlpService(config.params['nlp-service']['endpoint-url'],
          endpoint_request_mode=config.params['nlp-service']['endpoint-request-mode'],
          use_bulk_indexing=config.params['nlp-service']['use-bulk-indexing'],
          username=config.params['nlp-service']['credentials']['username'],
          password=config.params['nlp-service']['credentials']['password'],
          max_number_of_retries=config.params['nlp-service']['max-retries-on-failure'])

        # initialize the elastic sink
        sink_params = config.params['sink']

        sink_credentials = sink_params['es']['credentials'] if sink_params['es']['credentials'] else None
        sink_extra_params = sink_params['es']['extra-params'] if sink_params['es']['extra-params'] else None
        sink_security = sink_params['es']['security']  if sink_params['es']['security'] else None

        sink_ssl_config = SslConnectionConfig(ca_file_path=sink_security['ca-file-path'],
                                              ca_certs_path=sink_security['ca-certs-path'],
                                              client_cert_path=sink_security['client-cert-path'],
                                              client_key_path=sink_security['client-key-path'])

        es_sink_conf =  ElasticConnectorConfig(hosts=sink_params['es']['hosts'], credentials=sink_credentials, extra_params=sink_extra_params,
                                                   ssl_config=sink_ssl_config)

        es_sink_conn = ElasticConnector(es_sink_conf)
        es_sink = ElasticIndexer(es_sink_conn, sink_params['es']['index-name'])

        # initialize the indexer
        mapping = config.params['mapping']
        annoation_indexer_config = AnnotationIndexerConfig(nlp_service=nlp_service,
                                          source_indexer=es_source,
                                          source_text_field=mapping['source']['text-field'],
                                          source_docid_field=mapping['source']['docid-field'],
                                          source_fields_to_persist=mapping['source']['persist-fields'],
                                          sink_indexer=es_sink,
                                          split_index_by_field=mapping['sink']['split-index-by-field'],
                                          source_batch_date_field=mapping['source']['batch']['date-field'],
                                          batch_date_format=mapping['source']['batch']['date-format'],
                                          skip_doc_check=mapping['nlp']['skip-processed-doc-check'],
                                          nlp_ann_id_field=mapping['nlp']['annotation-id-field'],
                                          threads=mapping['source']['batch']['threads'],
                                          python_date_format=mapping['source']['batch']['python-date-format'],
                                          interval=mapping['source']['batch']['interval'],
                                          same_index_ingest=mapping['index-ingest-mode']['same-index'],
                                          use_nested_objects=mapping['index-ingest-mode']['use-nested-objects'],
                                          es_nested_object_schema_mapping=mapping['index-ingest-mode']['es-nested-object-schema-mapping'])
                                          
        indexer = BatchAnnotationsIndexer(annoation_indexer_config)

    except Exception as e:
        logging.error("Cannot initialize the application: " + str(e))
        exit(1)

    # set up the Elastic logger to be more verbose and run the indexer
    logging.getLogger('elasticsearch').setLevel(int(config.params['logging-level']))
    
    indexer.index_range(batch_date_start=mapping['source']['batch']['date-start'],
                        batch_date_end=mapping['source']['batch']['date-end'])

