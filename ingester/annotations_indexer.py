#!/usr/bin/python

import logging
# import time
from concurrent.futures import ThreadPoolExecutor


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
    MIN_TEXT_LEN = 10

    def __init__(self, nlp_service, source_indexer, source_text_field, source_docid_field,
                 source_fields_to_persist, sink_indexer, split_index_by_field="", threads=1, use_bulk_indexing=True,
                 skip_doc_check=False, nlp_ann_id_field='id'):
        """
        :param nlp_service: the NLP service to use :class:~`NlpService`
        :param source_indexer: the source ElasticSearch indexer :class:`~ElasticIndexer`
        :param source_text_field: the field in source documents containing the text to process
        :param source_fields_to_persist: the fields in source documents to persis in annotations
        :param sink_indexer: the sink ElasticSearch indexer :class:`~ElasticIndexer`
        :param split_index_by_field: optional, the name of the field by which the sink index should be split
        :param skip_doc_check: optional, whether to skip checking for already ingested documents
        :param nlp_ann_id_field: optional, the name of the annotation id field
        """
        self.nlp_service = nlp_service
        self.source_indexer = source_indexer
        self.source_text_field = source_text_field
        self.source_docid_field = source_docid_field
        self.source_fields_to_persist = source_fields_to_persist

        if source_docid_field not in source_fields_to_persist:
            self.source_fields_to_persist.append(source_docid_field)

        self.sink_indexer = sink_indexer

        self.split_index_by_field = split_index_by_field
        self.use_bulk_indexing = use_bulk_indexing
        self.threads = threads

        self.skip_doc_check = skip_doc_check
        self.nlp_ann_id_field = nlp_ann_id_field

        self.log = logging.getLogger('AnnotationsIndexer')

    def _get_doc_ids(self):
        """
        Returns the document IDs to be processed
        """
        return self.source_indexer.get_doc_ids_scan()

    def _document_already_processed(self, doc):
        """
        Checks whether specified document has been possibly already processed
        """
        if len(self.split_index_by_field) > 0:
            suffix = "*"
        else:
            suffix = ""

        # individual annotations will contain the original document id, hence the goal
        # is to check the value of the source document embedded in the annotation
        field_name = "%s.%s" % (self.FIELD_META_PREFIX, self.source_docid_field)
        match_criteria = {field_name: doc[self.source_docid_field]}

        return self.sink_indexer.doc_exists(match_criteria=match_criteria, index_suffix=suffix)

    def _index_annotations(self, nlp_response, document):
        """
        Indexes the annotations provided in the NLP Service response
        """
        annotations = nlp_response['annotations']

        for ann in annotations:
            # update annotation entry with fields from the document to persist and with prefix
            refined_ann = {}
            for field in self.source_fields_to_persist:
                if field in document:
                    refined_field = "%s.%s" % (self.FIELD_META_PREFIX, field)
                    refined_ann[refined_field] = document[field]

            # update annotation entry with fields from the annotation with prefix
            for field, value in ann.items():
                refined_field = "%s.%s" % (self.FIELD_ANN_PREFIX, field)
                refined_ann[refined_field] = value

            # index the refined annotation
            if len(self.split_index_by_field) > 0 and self.split_index_by_field in ann:
                self.sink_indexer.index_doc(doc=refined_ann, index_suffix=ann[self.split_index_by_field])
            else:
                self.sink_indexer.index_doc(refined_ann)

    def _prepare_annotations(self, annotations, document):
        """
        Returns a generator to create annotation documents -- used for ES bulk indexing
        """
        for ann in annotations:
            # update annotation entry with fields from the document to persist and with prefix
            refined_ann = {}
            for field in self.source_fields_to_persist:
                if field in document:
                    refined_field = "%s.%s" % (self.FIELD_META_PREFIX, field)
                    refined_ann[refined_field] = document[field]

            # update annotation entry with fields from the annotation with prefix
            for field, value in ann.items():
                refined_field = "%s.%s" % (self.FIELD_ANN_PREFIX, field)
                refined_ann[refined_field] = value

            if len(self.split_index_by_field) > 0:
                index_suffix = ann[self.split_index_by_field]
                index_name = self.sink_indexer.get_index_name(index_suffix)
            else:
                index_name = self.sink_indexer.get_index_name()

            operation = {
                '_id': "doc-%s-ann-%s" % (document[self.source_docid_field], ann[self.nlp_ann_id_field]),
                '_op_type': 'index',
                '_type': 'doc',
                '_index': index_name,
                '_source': refined_ann
            }
            yield operation

    def _index_annotations_bulk(self, nlp_response, document):
        """
        Indexes the annotations provided in the NLP Service response (bulk version)
        """
        annotations = nlp_response['annotations']
        self.sink_indexer.index_docs_bulk_gen(self._prepare_annotations(annotations, document))

    def _process_document(self, src_doc_id):
        """
        Performs full document processing cycle for the specified document id
        """
        self.log.info('Processing document with id: ' + src_doc_id)
        doc = self.source_indexer.get_doc(src_doc_id)

        # check whether there is document content to process
        if self.source_text_field not in doc or \
                doc[self.source_text_field] is None or \
                len(doc[self.source_text_field]) < self.MIN_TEXT_LEN:
            self.log.debug('- skipping: no content')
            return

        # check whether the document has been already processed
        if self.skip_doc_check and self._document_already_processed(doc):
            self.log.debug('- skipping: document already processed')
            return

        # get the text with metadata
        doc_text = doc[self.source_text_field]

        # query the NLP service and retrieve back the annotations
        # TODO: handle metadata and DCT
        # TODO: error handling
        # begin_t = time.time()
        self.log.debug('- querying the NLP service')
        nlp_response = self.nlp_service.query(text=doc_text)
        assert 'result' in nlp_response
        assert 'annotations' in nlp_response['result']

        if 'result' not in nlp_response:
            self.log.error(" - no result payload returned from NLP service")
            return

        if 'annotations' not in nlp_response['result'] or nlp_response['result']['annotations'] is None:
            self.log.error(" - no annotations available in the NLP result payload")
            return

        # self.log.info("-- took: %.3f s" % (time.time() - begin_t))
        # begin_t = time.time()

        self.log.info('- indexing annotations: %d' % len(nlp_response['result']['annotations']))

        if self.use_bulk_indexing:
            self._index_annotations_bulk(nlp_response['result'], doc)
        else:
            self._index_annotations(nlp_response['result'], doc)

        # self.log.info("-- took: %.3f s" % (time.time() - begin_t))

    def index(self):
        """
        Performs the indexing of annotations
        :param: source_date_start: the start date of documents to process
        :param: source_date_end: the end date of documents to process
        """
        self.log.info('Fetching document ids that match the criteria...')
        doc_ids = self._get_doc_ids()

        self.log.info('Found documents: %d' % len(doc_ids))
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
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

    def __init__(self, nlp_service, source_indexer, source_text_field, source_docid_field,
                 source_fields_to_persist, sink_indexer,
                 source_batch_date_field, batch_date_format="yyyy-MM-dd", split_index_by_field="", threads=1,
                 skip_doc_check=False, nlp_ann_id_field='id'):
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
        super().__init__(nlp_service, source_indexer, source_text_field, source_docid_field,
                         source_fields_to_persist, sink_indexer, split_index_by_field, threads, True,
                         skip_doc_check, nlp_ann_id_field)

        self.source_batch_date_field = source_batch_date_field
        self.batch_date_format = batch_date_format

    def _get_doc_ids_range(self, source_date_start, source_date_end):
        """
        Returns the document ids matching the specified range
        """
        return self.source_indexer.get_doc_ids_by_range_scan(date_field=self.source_batch_date_field,
                                                             date_format=self.batch_date_format,
                                                             date_begin=source_date_start,
                                                             date_end=source_date_end)

    def index_range(self, batch_date_start, batch_date_end):
        """
        Indexes the documents within the specified time range
        :param batch_date_start: the start date of the documents batch
        :param batch_date_end: the end date of the documents batch
        """
        self.log.info('Fetching document ids that match the criteria...')

        doc_ids = self._get_doc_ids_range(batch_date_start, batch_date_end)

        self.log.info('******************FIRST: Number of documents to annotate', len(doc_ids))

        doc_ids = set()
        args_list = []
        SLICES = min(3, self.threads * 20)
        for i in range(SLICES):
            args_list.append([i, SLICES, self.source_docid_field])
        with ThreadPoolExecutor(max_workers=SLICES) as exec:
            for result in exec.map(self.source_indexer.get_unique_field_values_slice, args_list):
                doc_ids.update(result)
        self.log.info('******************SECOND: Number of documents to annotate', len(doc_ids))

        processed_ids = set()
        field_name = "%s.%s" % (self.FIELD_META_PREFIX, self.source_docid_field)
        args_list = []
        for i in range(SLICES):
            args_list.append([i, SLICES, field_name])
        with ThreadPoolExecutor(max_workers=SLICES) as exec:
            for result in exec.map(self.sink_indexer.get_unique_field_values_slice, args_list):
                processed_ids.update(result)
        self.log.info('Number of documents being annotated', len(processed_ids))

        doc_ids = list(doc_ids - processed_ids)

        self.log.info('Found documents: %d' % len(doc_ids))
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self._process_document, doc_ids)
