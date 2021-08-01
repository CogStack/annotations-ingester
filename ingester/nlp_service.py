#!/usr/bin/python

import requests

from ingester.utils import check_url_available
import logging

################################
#
# nlp service
#
class NlpService:
    """
    The NLP service for querying the NLP REST API
    """
    def __init__(self, url_endpoint, endpoint_request_mode, use_bulk_indexing, username, password):
        """
        :param url_endpoint: the full url endpoint to query
        """
        self.log = logging.getLogger(self.__class__.__name__)

        assert url_endpoint is not None and len(url_endpoint) > 0

        if url_endpoint is None or len(url_endpoint) == 0 or not check_url_available(url_endpoint):
            raise Exception("Cannot connect to the provided REST service endpoint")

        self.endpoint_request_mode = endpoint_request_mode
        self.url_endpoint = url_endpoint
        self.use_bulk_indexing = use_bulk_indexing
        self.username = username
        self.password = password

    def query(self, text, metadata={}, application_params={}):
        """
        Sends the document to the NLP service to receive back the annotations
        :param text: the text to be processed
        :param metadata: metadata fields to be included with the response
        :param application_params: application parameters
        :return: returns the full NLP service response
        """

        if len(self.endpoint_request_mode) == 0:
            query_body = {
                "content": {
                    "text": text
                },
                "application_params": application_params,   
                "footer": metadata
            }
            response = requests.post(self.url_endpoint, json=query_body, headers={"Access-Control-Allow-Origin" : "*"}, auth=(self.username, self.password))
        elif self.endpoint_request_mode == 'gate-nlp':
            query_body = text
            response = requests.post(self.url_endpoint, data=query_body, headers={"Access-Control-Allow-Origin" : "*", "Content-Type": "text/plain"}, auth=(self.username, self.password))

        if response.status_code == 200:
            response = response.json()

            # Entities are present alone only when using GATE-NLP MODE ENDPOINT, they need formatting to match the MedCAT entities structure
            if self.endpoint_request_mode == 'gate-nlp':
                if "entities" in response.keys() and response["entities"] is not None:
                    tmp_ents = response["entities"]
                    formatted_result = {}
                    for entity_type in tmp_ents.keys():
                        for i in range(len(tmp_ents[entity_type])):
                            tmp_ents[entity_type][i].update({"type" : str(entity_type), "id" : i})
                            formatted_result[str(i)] = tmp_ents[entity_type][i]
                    response["entities"] = formatted_result

            return response
        else:
            self.log.warning("document did not return the correct response, status code: "
            + str(response.content)
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
