#!/usr/bin/python

import elasticsearch
import elasticsearch.helpers
import utils


################################
#
# connector config
#
class ElasticConnectorConfig:
    """
    ElasticSearch connector configuration
    At the moment supports only single-node clusters
    """
    def __init__(self, host, port, http_auth_user="", http_auth_pass=""):
        """
        :param host: the ElasticSearch host name
        :param port: the host's port
        :param http_auth_user: the user name in case of required HTTP/HTTPS authentication
        :param http_auth_pass: the user's password
        """
        self.host = host
        self.port = port
        self.http_user = http_auth_user
        self.http_pass = http_auth_pass


################################
#
# connector
#
class ElasticConnector:
    """
    ElasticSearch connector
    At the moment supports only single-node clusters
    """
    def __init__(self, elastic_conf):
        """
        :param elastic_conf: ElasticSearch configuration :class:`~ElasticConnectorConfig`
        """
        if len(elastic_conf.http_user) > 0:
            http_auth = "{user}:{password}".format(user=elastic_conf.http_user, password=elastic_conf.http_pass)
            self.es = elasticsearch.Elasticsearch(hosts=[{'host': elastic_conf.host, 'port': elastic_conf.port}],
                                                  http_auth=http_auth)
        else:
            self.es = elasticsearch.Elasticsearch(hosts=[{'host': elastic_conf.host, 'port': elastic_conf.port}])

        # check whether we can actually connect to ElasticSearch
        try:
            if self.es.ping() is not True:
                raise Exception("Cannot connect to ElasticSearch: %s:%s" % (elastic_conf.host, elastic_conf.port))
        except:
            raise Exception("Cannot connect to ElasticSearch: %s:%s" % (elastic_conf.host, elastic_conf.port))


################################
#
# indexer
#
class ElasticIndexer:
    """
    ElasticSearch indexer
    """
    def __init__(self, es_connector, index_name):
        """
        :param es_connector: ElasticSearch connector :class:`~ElasticConnector`
        :param index_name: the name of the index
        """
        self.conn = es_connector
        self.index_name = index_name

        # deprecated in 6.x so use it as a build-in param here
        self.doc_type = 'doc'

        self.index_name_cache = {}

    def _format_index_name(self, index_name):
        """
        ElasticSearch index name must follow certain rules:
        - must not contain the characters #, \, /, *, ?, ", <, >, |, ,
        - must not start with _, - or +
        - must not be . or ..
        - must be lowercase
        :param index_name: a possible index name
        :return: correct index name
        """
        return index_name.lower()\
            .strip('.').strip('_').strip('-').strip('+')\
            .replace('#', '_').replace('\\', '_').replace('\/', '_')\
            .replace('*', '_').replace('?', '_').replace('"', '_')\
            .replace('<', '_').replace('>', '_').replace('|', '_')\
            .replace(' ', '_')

    def _get_index_name(self, suffix="", search_only=False):
        """
        Returns the valid index name taking into account suffix and naming restrictions
        :param suffix: an optional index suffix name
        :return: valid index name
        """
        if len(suffix) > 0:
            if search_only and suffix is "*":
                return "%s-*" % self.index_name

            index_name = "%s-%s" % (self.index_name, suffix)
        else:
            index_name = self.index_name

        if index_name not in self.index_name_cache:
            self.index_name_cache[index_name] = self._format_index_name(index_name)

        return self.index_name_cache[index_name]

    def get_doc_count(self, index_suffix=""):
        """
        Queries the index for the number of documents (_count)
        :param index_suffix: an optional suffix of the index to query
        :return: the number of records
        """
        res = self.conn.es.count(self._get_index_name(suffix=index_suffix, search_only=True))
        return int(res['count'])

    def drop_index(self, index_suffix=""):
        """
        Drops the specified index
        :param index_suffix: optional suffix of the index to drop
        """
        self.conn.es.indices.delete(index=self._get_index_name(index_suffix))

    def index_doc(self, doc, doc_id=None, index_suffix=""):
        """
        Indexes the given document under under specified id
        :param doc: the body of the document to be stored (must be represented as KVPs dictionary)
        :param doc_id: the id of the document
        :param index_suffix: optional suffix of the index to store the document
        """
        self.conn.es.index(index=self._get_index_name(index_suffix), doc_type=self.doc_type, id=doc_id, body=doc)

    def index_docs_bulk(self, docs, index_suffix=""):
        """
        Indexes the documents using ElasticSearch bulk API
        :param docs: the array of documents, each represented as KVPs
        :param index_suffix: an optional index suffix name
        """
        index_name = self._get_index_name(index_suffix)
        elasticsearch.helpers.bulk(self.conn.es, docs, index=index_name, doc_type=self.doc_type)

    def get_doc(self, doc_id, index_suffix=""):
        """
        Retrieves the given document from a given index with a specified id
        :param doc_id: the id of the document
        :param index_suffix: optional suffix of the index to store the document
        :return: the document represented as KVPs dictionary
        """
        res = self.conn.es.get(index=self._get_index_name(index_suffix), doc_type=self.doc_type, id=doc_id)
        assert '_source' in res
        return res['_source']

    def get_doc_ids(self, index_suffix=""):
        """
        Retrieves the ids of all the documents
        :param index_suffix: optional suffix of the index to store the document
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

        meta = self.conn.es.search(index=self._get_index_name(suffix=index_suffix, search_only=True),
                                   body=query_body)
        if 'hits' not in meta:
            return []

        ids = [hit['_id'] for hit in meta['hits']['hits']]
        return ids

    def doc_exists(self, match_criteria, index_suffix=""):
        """
        Checks whether the document exists according to match criterion
        :param match_criteria: the match criteria specified as field-value KVPs
        :param index_suffix: optional suffix of the index to store the document
        :return: True if document exists, False otherwise
        """
        query_body = {
            "query": {
                "match": match_criteria
            }
        }
        res = self.conn.es.count(index=self._get_index_name(suffix=index_suffix, search_only=True),
                                 body=query_body)
        return int(res['count']) > 0

    def get_doc_ids_scan(self, index_suffix=""):
        """
        Retrieves the ids of all the documents using ElasticSearch scan API
        :param index_suffix: optional suffix of the index to store the document
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "match_all": {}
            },
            "stored_fields": []
        }

        ids_generator = elasticsearch.helpers.scan(self.conn.es,
                                                   query=query_body,
                                                   index=self._get_index_name(suffix=index_suffix,
                                                                              search_only=True),
                                                   doc_type=self.doc_type)
        ids = [hit['_id'] for hit in ids_generator]
        return ids


################################
#
# ranged indexer
#
class ElasticRangedIndexer(ElasticIndexer):
    def __init__(self, es_connector, index_name):
        super().__init__(es_connector, index_name)

    def get_doc_ids_by_range_scan(self, date_field, date_begin, date_end, date_format="yyyy-MM-dd", index_suffix=""):
        """
        Retrieves the ids of all the documents using ElasticSearch scan API
        :param date_field: the name of the field containing the date
        :param date_begin: begin of the range, inclusive
        :param date_end: optional suffix of the index to store the document
        :param date_format: the format of the date field
        :param index_suffix: end of the range, inclusive
        :return: the document ids in array
        """
        query_body = {
            "query": {
                "range": {
                    date_field: {
                        "gte": date_begin,
                        "lte": date_end,
                        "format": date_format
                    }
                }
            },
            "stored_fields": []
        }

        ids_generator = elasticsearch.helpers.scan(self.conn.es,
                                                   query=query_body,
                                                   index=self._get_index_name(index_suffix),
                                                   doc_type=self.doc_type)
        ids = [hit['_id'] for hit in ids_generator]
        return ids
