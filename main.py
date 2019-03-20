#!/usr/bin/python

from es_common import *
from nlp_service import *
from annotations_indexer import *

import logging
import argparse
import yaml


class AppConfig:
    """
    The configuration file for the indexer application
    """
    def __init__(self, file_path):
        """
        :param: filepath: the path for the configuration file stored in YAML
        """
        with open(file_path) as conf_file:
            yaml_file = yaml.safe_load(conf_file)

            if 'source' not in yaml_file or \
                    'nlp-service' not in yaml_file or \
                    'sink' not in yaml_file or \
                    'mapping' not in yaml_file:
                raise Exception("Invalid configuration file provided")

            self.params = yaml_file


if __name__ == "__main__":

    # parse the input parameters
    parser = argparse.ArgumentParser(description='ES2ES annotations indexer')
    parser.add_argument('--config', help='configuration file path')
    args = parser.parse_args()

    if args.config is None:
        print('No config file specified')
        exit(0)

    config = AppConfig(args.config)

    # setup logging
    log_format = '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=log_format, level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(level=logging.WARN)

    # initialize the elastic source
    source_params = config.params['source']
    es_source_conf = ElasticConnectorConfig(host=source_params['es']['host-name'],
                                            port=source_params['es']['host-port'])
    es_source_conn = ElasticConnector(es_source_conf)

    es_source = ElasticRangedIndexer(es_source_conn, source_params['es']['index-name'])

    # initialize NLP service
    nlp_service = BioyodieService(config.params['nlp-service']['endpoint-url'])

    # initialize the elastic sink
    sink_params = config.params['sink']
    es_sink_conf = ElasticConnectorConfig(host=sink_params['es']['host-name'],
                                          port=sink_params['es']['host-port'])
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
                                      batch_date_format=mapping['source']['batch']['date-format'])

    # run the indexer
    indexer.index_range(batch_date_start=mapping['source']['batch']['date-start'],
                        batch_date_end=mapping['source']['batch']['date-end'])

