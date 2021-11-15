#!/usr/bin/python

from ingester.utils import remove_duplicate_records
from ingester.nlp_service import NlpService
from ingester.es_common import ElasticIndexer, ElasticRangedIndexer
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time
from datetime import timedelta

from dataclasses import dataclass

import json
import ast
import re
import itertools

@dataclass
class AnnotationIndexerConfig:
    """
        :param nlp_service: the NLP service to use :class:~`NlpService`
        :param source_indexer: the source ElasticSearch indexer :class:`~ElasticIndexer`
        :param source_text_field: the field in source documents containing the text to process
        :param source_fields_to_persist: the fields in source documents to persis in annotations
        :param sink_indexer: the sink ElasticSearch indexer :class:`~ElasticIndexer`
        :param source_batch_date_field: the field in source documents containing the date field to select documents
        :param batch_date_format: the format of the batch dates specified
        :param split_index_by_field: optional, the name of the field by which the sink index should be split
        :param skip_doc_check: optional, whether to skip checking for already ingested documents
        :param nlp_ann_id_field: optional, the name of the annotation id field
    """
     
    nlp_service : NlpService = None
    source_indexer : ElasticRangedIndexer = None 
    source_text_field : str = ""
    source_docid_field : str = ""
    source_fields_to_persist : str = ""
    sink_indexer : ElasticIndexer = None
    split_index_by_field : str = ""
    threads : int = 4
    skip_doc_check : bool = False
    nlp_ann_id_field : str = "id"
    python_date_format : str = '%Y-%m-%d'
    source_batch_date_field : str = ""
    batch_date_format: str = "yyyy-MM-dd"
    interval : int = 30
    same_index_ingest : bool = False
    use_nested_objects : bool = False
    es_nested_object_schema_mapping : str = ""

################################
#
# standard annotations indexer
#
class AnnotationsIndexer:
    """
    The ElasticSearch Annotations indexer
    Performs: ES --> NLP Service --> ES indexing
    """

    # predefined prefixes for the fields when sending the NLP result to ElasticSearch
    FIELD_ANN_PREFIX = "nlp"
    FIELD_META_PREFIX = "meta"

    # minimum length of the text field that will be sen to ElasticSearch
    MIN_TEXT_LEN = 5

    def __init__(self, annotation_indexer_config : AnnotationIndexerConfig = AnnotationIndexerConfig()):

        self.annotation_indexer_config = annotation_indexer_config

        self.log = logging.getLogger(self.__class__.__name__)
     
    def _get_doc_ids(self):
        """
        Returns the document IDs to be processed
        """
        return self.annotation_indexer_config.source_indexer.get_doc_ids_scan()

    def _document_already_processed(self, doc):
        """
        Checks whether specified document has been possibly already processed
        """
     
        if self.annotation_indexer_config.same_index_ingest:
            if "annotations" in list(doc.keys()):
                if len(doc["annotations"]) > 0:
                    return True
            return False
        else:
            if len(self.annotation_indexer_config.split_index_by_field) > 0:
                suffix = "*"
            else:
                suffix = ""

            # individual annotations will contain the original document id, hence the goal
            # is to check the value of the source document embedded in the annotation
            field_name = "%s.%s" % (self.FIELD_META_PREFIX, self.annotation_indexer_config.source_docid_field)
            match_criteria = {field_name: doc[self.annotation_indexer_config.source_docid_field]}

            return self.annotation_indexer_config.sink_indexer.doc_exists(match_criteria=match_criteria, index_suffix=suffix)

    def _index_annotations(self, annotations, document, src_doc_id):
        """
        Indexes the annotations provided in the NLP Service response
        """
        updated_annotations = []
        updated_annotations.extend(annotations.values())
        
        if "annotations" in document.keys():
          updated_annotations.extend(document["annotations"])

        if self.annotation_indexer_config.same_index_ingest:
            self.annotation_indexer_config.source_indexer.conn.es.update(index=self.annotation_indexer_config.source_indexer.get_index_name(),
             body = {"doc" : {"annotations" : updated_annotations}})
        elif self.annotation_indexer_config.use_nested_objects:
          document["annotations"] = updated_annotations
          self.annotation_indexer_config.sink_indexer.index_doc(document, doc_id=src_doc_id + "_annotations")
        else:
            for ann in annotations:
                # update annotation entry with fields from the document t4o persist and with prefix
                refined_ann = {}
                if self.annotation_indexer_config.source_fields_to_persist:
                  for field in self.annotation_indexer_config.source_fields_to_persist:
                    if field in document:
                        refined_field = "%s.%s" % (self.FIELD_META_PREFIX, field)
                        refined_ann[refined_field] = document[field]

                # update annotation entry with fields from the annotation with prefix
                for field, value in ann.items():
                    refined_field = "%s.%s" % (self.FIELD_ANN_PREFIX, field)
                    refined_ann[refined_field] = value

                # index the refined annotation
                if len(self.annotation_indexer_config.split_index_by_field) > 0 and self.annotation_indexer_config.split_index_by_field in ann:
                    self.annotation_indexer_config.sink_indexer.index_doc(doc=refined_ann, index_suffix=ann[self.annotation_indexer_config.split_index_by_field])
                else:
                    self.annotation_indexer_config.sink_indexer.index_doc(refined_ann)

    def _prepare_annotations(self, annotations_entities, document, src_doc_id):
        """
            Returns a generator to create annotation documents -- used for ES bulk indexing
        """
        # if we choose to ingest back into the same index we create an extra field
        refined_annotations = []
        refined_annotations.extend(annotations_entities.values())

        if self.annotation_indexer_config.same_index_ingest:
          
          # check if document has annotations already
          if "annotations" in document.keys():
            refined_annotations.extend(document["annotations"])
            refined_annotations = remove_duplicate_records(refined_annotations)

          operation = {
                  "_id": src_doc_id,
                  "_op_type": "update",
                  "script" : {
                      "lang": "painless",
                      "source" : "ctx._source.annotations = new ArrayList(); ctx._source.annotations = params.annotations",
                      "params" : {"annotations" : refined_annotations}
                  },
                  "_index" : self.annotation_indexer_config.source_indexer.get_index_name()
              }
          yield operation
        elif self.annotation_indexer_config.use_nested_objects:
            ann_doc_id = "doc_" + str(document[self.annotation_indexer_config.source_docid_field]) + "_annotations"
            already_exists = self.annotation_indexer_config.sink_indexer.doc_exists({"_id": ann_doc_id})

            if already_exists:
              ann_doc = self.annotation_indexer_config.sink_indexer.get_doc(ann_doc_id)
              
              if "annotations" in ann_doc.keys():
                refined_annotations.extend(ann_doc["annotations"])
                refined_annotations = remove_duplicate_records(refined_annotations)
                operation = {
                  "_id": ann_doc_id,
                  "_op_type":  'update',
                  "script" : {
                      "lang": "painless",
                      "source" : "ctx._source.annotations = new ArrayList(); ctx._source.annotations = params.annotations",
                      "params" : {"annotations" : refined_annotations}
                  },
                  "_index" : self.annotation_indexer_config.sink_indexer.get_index_name()
                }
            else:
              fields_to_persist = {}
              if self.annotation_indexer_config.source_fields_to_persist:
                for field in self.annotation_indexer_config.source_fields_to_persist:
                  if field in document.keys():
                      refined_field = "%s.%s" % (self.FIELD_META_PREFIX, field)
                      fields_to_persist[refined_field] = document[field]

              operation = {
                      "_id": ann_doc_id,
                      "_op_type":  'index',
                      "_source" : {"annotations" : refined_annotations, **fields_to_persist},
                      "_index" : self.annotation_indexer_config.sink_indexer.get_index_name()
              }
            yield operation
        else:
          for index, entity in annotations_entities.items():
              refined_ann = {}
              if self.annotation_indexer_config.source_fields_to_persist:
                for field in self.annotation_indexer_config.source_fields_to_persist:
                  if field in document:
                      refined_field = "%s.%s" % (self.FIELD_META_PREFIX, field)
                      refined_ann[refined_field] = document[field]

              for field, value in entity.items():
                  refined_field = "%s.%s" % (self.FIELD_ANN_PREFIX, field)
                  refined_ann[refined_field] = value

              if len(self.annotation_indexer_config.split_index_by_field) > 0:
                  index_suffix = entity[self.annotation_indexer_config.split_index_by_field]
                  index_name = self.annotation_indexer_config.sink_indexer.get_index_name(index_suffix)
              else:
                  index_name = self.annotation_indexer_config.sink_indexer.get_index_name()

              operation = {
                  '_id': "doc-%s-ann-%s" % (document[self.annotation_indexer_config.source_docid_field], entity[self.annotation_indexer_config.nlp_ann_id_field]),
                  '_op_type': 'index',
                  '_index': index_name,
                  '_source': refined_ann
              }

              yield operation

    def _index_annotations_bulk(self, annotations, document, src_doc_id):
        """
        Indexes the annotations provided in the NLP Service response (bulk version)
        """
        self.annotation_indexer_config.sink_indexer.index_docs_bulk_gen(self._prepare_annotations(annotations, document, src_doc_id))

    def _process_document(self, src_doc_id):
        """
        Performs full document processing cycle for the specified document id
        """

        self.log.info('Processing document with id: ' + src_doc_id)
        doc = self.annotation_indexer_config.source_indexer.get_doc(src_doc_id)


        # check whether there is document content to process
        if (isinstance(doc, dict) and (self.annotation_indexer_config.source_text_field not in doc.keys() or doc[self.annotation_indexer_config.source_text_field] is None)) or \
          len(doc[self.annotation_indexer_config.source_text_field]) < self.MIN_TEXT_LEN:
            self.log.info('- skipping: no content')
            return
        
        try:
            # check whether the document has been already processed
            if self.annotation_indexer_config.skip_doc_check and self._document_already_processed(doc):
                self.log.info('doc id : ' + str(src_doc_id) + ' - skipping: document already processed')
                return
         
            # get the text
            doc_text = doc[self.annotation_indexer_config.source_text_field]
    
            # query the NLP service and retrieve back the annotations
            self.log.info('- querying the NLP service')
            
            nlp_response = self.annotation_indexer_config.nlp_service.query(text=doc_text)

            self.log.info("Finished processing NLP for document with id: " + src_doc_id)

            if "result" in nlp_response.keys():
              result = nlp_response["result"]

              if 'annotations' not in result.keys() or result['annotations'] is None or result is None:
                self.log.error(" - no annotations available in the NLP result payload")
                return 

              if 'entities' not in result["annotations"].keys() or result["annotations"]["entities"] is None :
                self.log.error(" - no annotation entities available in the NLP result payload")
                return

              result = result['annotations']['entities']
        
            elif "entities" in nlp_response.keys():
              # Entities are present alone only when using GATE-NLP MODE ENDPOINT
              if nlp_response["entities"] is not None:
                result = nlp_response["entities"]
              else:
                self.log.error(" - no annotation entities available in the NLP result payload")
                return

            elif "result" not in nlp_response.keys() or "entities" not in nlp_response.keys():
                self.log.error(" - no result payload returned from NLP service")
                return

            if self.annotation_indexer_config.nlp_service.use_bulk_indexing:
                self._index_annotations_bulk(result, doc, src_doc_id)
            else:
                self._index_annotations(result, doc, src_doc_id)
           
        except Exception as e:
            self.log.error(repr(e))
      

    def index(self):
        """
        Performs the indexing of annotations
        :param: source_date_start: the start date of documents to process
        :param: source_date_end: the end date of documents to process
        """
        self.log.info('Fetching document ids that match the criteria...')
        doc_ids = self._get_doc_ids()

        self.log.info('Found documents: %d' % len(doc_ids))

        with ThreadPoolExecutor(max_workers=self.annotation_indexer_config.threads) as executor:
            executor.map(self._process_document, doc_ids)


################################
#
# batch-like annotations indexer
#
class BatchAnnotationsIndexer(AnnotationsIndexer):
    """
    The batch version of ElasticSearch Annotations indexer
    Performs: ES --> NLP Service --> ES indexing
    """

    def __init__(self, annotation_indexer_config):
        super().__init__(annotation_indexer_config)

    def _get_doc_ids_range(self, source_date_start, source_date_end):
        """
        Returns the document ids matching the specified range
        """
        return self.annotation_indexer_config.source_indexer.get_doc_ids_by_range_scan(date_field=self.annotation_indexer_config.source_batch_date_field,
                                                             date_format=self.annotation_indexer_config.batch_date_format,
                                                             date_begin=source_date_start,
                                                             date_end=source_date_end)

    def index_range(self, batch_date_start, batch_date_end):
        """
        Indexes the documents within the specified time range
        :param batch_date_start: the start date of the documents batch
        :param batch_date_end: the end date of the documents batch
        """
        
        request_body = {}

        # if we are going to ingest into the same index, we must make sure that the annotations field exists and is of the correct type
        if self.annotation_indexer_config.same_index_ingest:
            mappings = self.annotation_indexer_config.source_indexer.conn.es.indices.get_mapping(index=self.annotation_indexer_config.source_indexer.get_index_name())
            
            if "annotations" not in mappings[self.annotation_indexer_config.source_indexer.get_index_name()]["mappings"]["properties"]:
              request_body = {
                          "properties": {
                                  "annotations": {
                                      "type" : "nested" if self.annotation_indexer_config.use_nested_objects else "flattened"
                              }
                          }
                      }

        if self.annotation_indexer_config.es_nested_object_schema_mapping.lower() == "medcat-nested-object":
          request_body = {
              "properties": {
                "annotations": {
                  "type" : "nested",
                  "properties": {
                    "acc": {
                      "type": "float"
                    },
                    "context_similarity": {
                      "type": "float"
                    },
                    "cui": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "detected_name": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "end": {
                      "type": "long"
                    },
                    "id": {
                      "type": "long"
                    },
                    "meta_anns" : {
                      "type" : "nested"
                    },
                    "pretty_name": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "source_value": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "start": {
                      "type": "long"
                    },
                    "tuis": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "types": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                }
                }
              }
            }
      
        if self.annotation_indexer_config.es_nested_object_schema_mapping.lower() == "gate-nlp-nested-object":
            request_body = {
                    "properties": {
                      "annotations": {
                        "type" : "nested",
                        "properties": {
                            "NMRule": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "firstName": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "gender": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "id": {
                              "type": "long",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "indices": {
                              "type": "long"
                            },
                            "initials": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "kind": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "matchedWithLonger": {
                              "type": "boolean"
                            },
                            "matches": {
                              "type": "long"
                            },
                            "orgType": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "orgType ": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "rule": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "rule ": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "ruleFinal": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "surname": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "title": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "type": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            }
                          }
                      }
                    }
                  }

        if self.annotation_indexer_config.es_nested_object_schema_mapping.lower() == "medcat-separate-index":
          request_body = {
            "properties": {
                    "annotations": {
                      "properties": {
                        "acc": {
                          "type": "float"
                        },
                        "context_similarity": {
                          "type": "float"
                        },
                        "cui": {
                          "type": "keyword",
                          "ignore_above": 64
                        },
                        "detected_name": {
                          "type": "text",
                          "fields": {
                            "keyword": {
                              "type": "keyword",
                              "ignore_above": 256
                            }
                          }
                        },
                        "end": {
                          "type" : "keyword",
                          "store" : "true",
                          "index" : "false"
                        },
                        "id": {
                          "type": "keyword"
                        },
                        "meta_anns": {
                          "properties": {
                            "Status": {
                              "properties": {
                                "confidence": {
                                  "type": "float"
                                },
                                "name": {
                                  "type": "text",
                                  "fields": {
                                    "keyword": {
                                      "type": "keyword",
                                      "ignore_above": 256
                                    }
                                  }
                                },
                                "value": {
                                  "type": "text",
                                  "fields": {
                                    "keyword": {
                                      "type": "keyword",
                                      "ignore_above": 256
                                    }
                                  }
                                }
                              }
                            }
                          }
                        },
                        "pretty_name": {
                          "type": "text",
                          "fields": {
                            "keyword": {
                              "type": "keyword",
                              "ignore_above": 256
                            }
                          }
                        },
                        "source_value": {
                          "type": "text",
                          "fields": {
                            "keyword": {
                              "type": "keyword",
                              "ignore_above": 256
                            }
                          }
                        },
                        "start": {
                          "type" : "keyword",
                          "store" : "true",
                          "index" : "false"
                        },
                        "tuis": {
                          "type": "keyword",
                          "ignore_above": 64
                        },
                        "types": {
                          "type": "text",
                          "fields": {
                            "keyword": {
                              "type": "keyword",
                              "ignore_above": 256
                            }
                          }
                        }
                      }
                    }
                  }
                }
        
        if self.annotation_indexer_config.es_nested_object_schema_mapping.lower() == "gate-nlp-separate-index":
          request_body = {
                        "properties": {
                          "annotations": {
                            "properties": {
                              "NMRule": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "firstName": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "gender": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "id": {
                                "type": "long",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "indices": {
                                "type": "long"
                              },
                              "initials": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "kind": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "matchedWithLonger": {
                                "type": "boolean"
                              },
                              "matches": {
                                "type": "long"
                              },
                              "orgType": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "orgType ": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "rule": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "rule ": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "ruleFinal": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "surname": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "title": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              },
                              "type": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              }
                            }
                          }
                        }
                      }

        if self.annotation_indexer_config.same_index_ingest:
          self.annotation_indexer_config.source_indexer.conn.es.indices.put_mapping(body=json.dumps(request_body), index=self.annotation_indexer_config.source_indexer.get_index_name())
        elif  self.annotation_indexer_config.es_nested_object_schema_mapping != "":
          if not self.annotation_indexer_config.sink_indexer.conn.es.indices.exists(index=self.annotation_indexer_config.sink_indexer.get_index_name()):
            self.annotation_indexer_config.sink_indexer.conn.es.indices.create(index=self.annotation_indexer_config.sink_indexer.get_index_name())
          self.annotation_indexer_config.sink_indexer.conn.es.indices.put_mapping(body=json.dumps(request_body), index=self.annotation_indexer_config.sink_indexer.get_index_name())

        continue_read = True
 
        seg_batch_date_start = batch_date_start
        seg_batch_date_end = batch_date_start
 
        while continue_read:
            seg_batch_date_start = seg_batch_date_end
            dt_seg_batch_date_end = datetime.strptime(seg_batch_date_start, self.annotation_indexer_config.python_date_format) + timedelta(days=self.annotation_indexer_config.interval)
            seg_batch_date_end = dt_seg_batch_date_end.strftime(self.annotation_indexer_config.python_date_format)
 
            if dt_seg_batch_date_end >= datetime.strptime(batch_date_end, self.annotation_indexer_config.python_date_format):
                seg_batch_date_end = batch_date_end
                continue_read = False
 
            self.log.info('Fetching document ids that match the criteria... ' + seg_batch_date_start + ' - ' + seg_batch_date_end)
            doc_ids = self._get_doc_ids_range(seg_batch_date_start, seg_batch_date_end)
          
            self.log.info('Found documents: %d' % len(doc_ids))
            with ThreadPoolExecutor(max_workers=self.annotation_indexer_config.threads) as executor:
                executor.map(self._process_document, doc_ids)
