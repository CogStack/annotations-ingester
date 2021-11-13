#!/usr/bin/python

from datetime import datetime
from ingester.utils import check_url_available
import logging
import json
import requests
import traceback

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

        self.endpoint_request_mode = endpoint_request_mode
        self.url_endpoints = url_endpoint
        self.use_bulk_indexing = use_bulk_indexing
        self.username = username
        self.password = password

        self.max_number_of_retries = 3 
    
        if type(self.url_endpoints) is not list:
            self.url_endpoints = [self.url_endpoints]
        
        assert self.url_endpoints is not None and len(self.url_endpoints) > 0

        if self.url_endpoints is None or len(self.url_endpoints) == 0 or not check_url_available(self.url_endpoints):
            raise Exception("Cannot connect to the provided REST service endpoint")

    def query(self, text, metadata={}, application_params={}):
        """
        Sends the document to the NLP service to receive back the annotations
        :param text: the text to be processed
        :param metadata: metadata fields to be included with the response
        :param application_params: application parameters
        :return: returns the full NLP service response
        """

        try:
            headers = {"Access-Control-Allow-Origin" : "*", "Content-Type": "application/json"}
            auth = (self.username, self.password)
            query_body = {}

            request_responses = []
            final_response = {}

            if len(self.endpoint_request_mode) == 0:
                query_body = {
                    "content": {
                        "text": text
                    },
                    "application_params": application_params,   
                    "footer": metadata
                }
                query_body = json.dumps(query_body)

            elif self.endpoint_request_mode == 'gate-nlp':
                query_body = text
                headers = {"Access-Control-Allow-Origin" : "*", "Content-Type": "text/plain"}

            for url in self.url_endpoints:
                self.log.info("Requesting to " + url)
                request = requests.post(url, data=query_body, headers=headers, auth=auth)

                number_of_retries = 0

                while(request.status_code != 200 and number_of_retries < self.max_number_of_retries):
                    self.log.info("Request to " + url + " failed, retrying")
                    request = requests.post(url, data=query_body, headers=headers, auth=auth)
                    number_of_retries += 1

                if request.status_code == 200:
                    current_request = request.json()
                    if self.endpoint_request_mode == 'gate-nlp' and current_request:
                        current_request.update({"pipeline_url" : str(url)})
                    if current_request:
                        request_responses.append(current_request)
                else:
                    self.log.warning("document did not return the correct response, status code:"
                    + str(request.content)
                    + str(request.status_code) + "  " + request.reason + "\n The document will be reprocessed at the next check")
                    request_responses.append({})



            annotation_index = 0

            current_timestamp = datetime.now().strftime("%H:%M:%S")

            for response in request_responses:
                if "result" in response.keys():
                    if type(response["result"]) is not dict:
                        response["result"] =json.loads(response["result"])

                    if "medcat_info" in response.keys() and "annotations" in response["result"].keys():
                        for k in response["result"]["annotations"]["entities"].keys():
                            response["result"]["annotations"]["entities"][k].update(response["medcat_info"])
                            response["result"]["annotations"]["entities"][k].update({"timestamp" : response["result"]["timestamp"]})
                    final_response = response

                # Entities are present alone only when using GATE-NLP MODE ENDPOINT, they need formatting to match the MedCAT entities structure
                if self.endpoint_request_mode == 'gate-nlp':
                    if "entities" in response.keys() and response["entities"] is not None:
                        tmp_ents = response["entities"]
                        formatted_result = {}
                        for entity_type in tmp_ents.keys():
                            for i in range(len(tmp_ents[entity_type])):
                                annotation_indices = list(map(int, tmp_ents[entity_type][i]["indices"]))

                                tmp_ents[entity_type][i].update({"type" : str(entity_type), "id" : annotation_index, "pipeline_url" : response["pipeline_url"], "timestamp" : current_timestamp,
                                 "source_value" : response["text"][annotation_indices[0]:annotation_indices[1]]})

                                formatted_result[str(annotation_index)] = tmp_ents[entity_type][i]
                                annotation_index += 1
                        response["entities"] = formatted_result

                    for k, v in response.items():
                        if k in final_response.keys():
                            if type(v) is dict:
                                final_response[k].update(v)
                        elif k != "pipeline_url":
                            final_response[k] = v

            return final_response
        except Exception:
            logging.error(traceback.print_exc())


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
