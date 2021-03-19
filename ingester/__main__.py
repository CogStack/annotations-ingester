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

    # setup logging
    log_format = '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)

    try:
        config = AppConfig(args.config)

        # set up Elastic logger for the initialization time
        logging.getLogger('elasticsearch').setLevel(level=logging.ERROR)

        # initialize the elastic source
        source_params = config.params['source']
        if 'security' in source_params['es']:
            src_sec = source_params['es']['security']
            source_ssl_config = SslConnectionConfig(ca_certs_path=src_sec['ca-certs-path'],
                                                    client_cert_path=src_sec['client-cert-path'],
                                                    client_key_path=src_sec['client-key-path'])
            es_source_conf = ElasticConnectorConfig(hosts=source_params['es']['hosts'],
                                                    ssl_config=source_ssl_config)
        else:
            es_source_conf = ElasticConnectorConfig(hosts=source_params['es']['hosts'])
        es_source_conn = ElasticConnector(es_source_conf)

        es_source = ElasticRangedIndexer(es_source_conn, source_params['es']['index-name'])

        # initialize NLP service
        nlp_service = NlpService(config.params['nlp-service']['endpoint-url'])

        # initialize the elastic sink
        sink_params = config.params['sink']
        if 'security' in sink_params['es']:
            sink_sec = sink_params['es']['security']
            sink_ssl_config = SslConnectionConfig(ca_certs_path=sink_sec['ca-certs-path'],
                                                  client_cert_path=sink_sec['client-cert-path'],
                                                  client_key_path=sink_sec['client-key-path'])

            es_sink_conf = ElasticConnectorConfig(hosts=source_params['es']['hosts'],
                                                  ssl_config=sink_ssl_config)
        elif 'extra_params' in sink_params['es']:
            e_params = sink_params['es']['extra_params']
            es_sink_conf = ElasticConnectorConfig(hosts=sink_params['es']['hosts'], extra_params=e_params)
        else:
            es_sink_conf = ElasticConnectorConfig(hosts=sink_params['es']['hosts'])
        es_sink_conn = ElasticConnector(es_sink_conf)
        es_sink = ElasticIndexer(es_sink_conn, sink_params['es']['index-name'])


        # initialize the indexer
        mapping = config.params['mapping']
        indexer = BatchAnnotationsIndexer(nlp_service=nlp_service,
                                          source_indexer=es_source,
                                          source_text_field=mapping['source']['text-field'],
                                          source_docid_field=mapping['source']['docid-field'],
                                          source_fields_to_persist=mapping['source']['persist-fields'],
                                          split_index_by_field=mapping['sink']['split-index-by-field'],
                                          sink_indexer=es_sink,
                                          source_batch_date_field=mapping['source']['batch']['date-field'],
                                          batch_date_format=mapping['source']['batch']['date-format'],
                                          skip_doc_check=mapping['nlp']['skip-processed-doc-check'].lower() == "true",
                                          nlp_ann_id_field=mapping['nlp']['annotation-id-field'],
                                          threads=mapping['source']['batch']['threads'],
                                          python_date_format=mapping['source']['batch']['python-date-format'],
                                          interval=mapping['source']['batch']['interval'])
    except Exception as e:
        log = logging.getLogger('main')
        log.error("Cannot initialize the application: " + str(e))
        exit(1)

    # set up the Elastic logger to be more verbose and run the indexer
    logging.getLogger('elasticsearch').setLevel(level=logging.WARN)
    
    indexer.index_range(batch_date_start=mapping['source']['batch']['date-start'],
                        batch_date_end=mapping['source']['batch']['date-end'])

