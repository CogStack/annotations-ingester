#!/usr/bin/python

import requests

from ingester.utils import check_url_available
import json
import logging

################################
#
# nlp service
#
class NlpService:
    """
    The NLP service for querying the NLP REST API
    """
    def __init__(self, url_endpoint):
        """
        :param url_endpoint: the full url endpoint to query
        """
        self.log = logging.getLogger(self.__class__.__name__)

        assert url_endpoint is not None and len(url_endpoint) > 0
        if url_endpoint is None or len(url_endpoint) == 0 or not check_url_available(url_endpoint):
            raise Exception("Cannot connect to the provided REST service endpoint")

        self.url_endpoint = url_endpoint

    def query(self, text, metadata={}, application_params={}):
        """
        Sends the document to the NLP service to receive back the annotations
        :param text: the text to be processed
        :param metadata: metadata fields to be included with the response
        :param application_params: application parameters
        :return: returns the full NLP service response
        """
        query_body = {
            "content": {
                "text": text
            },
            "application_params": application_params,
            "footer": metadata
        }

        response = requests.post(self.url_endpoint, json=query_body)

        if response.status_code == 200:
            return response.json()
        else:
            self.log.warning("document did not return the correct response, status code: "
             + str(response.status_code) + "  " + response.reason + "\n The document will be reprocessed at the next check")

        return {}


################################
#
# bioyodie service
#
class BioyodieService(NlpService):
    """
    The NLP BioYodie service
    """
    def __init__(self, url_endpoint):
        """
        :param url_endpoint: the full url endpoint to query
        """
        super().__init__(url_endpoint)

    def query(self, text, metadata={}, application_params={'annotationSets': "Bio:*"}):
        """
        Sends the document to the NLP service to receive back the annotations
        :param text: the text to be processed
        :param metadata: metadata fields to be included with the response
        :param application_params: the NLP application runtime params
        :return: returns the full NLP service response
        """
        return super().query(text, metadata, application_params)
