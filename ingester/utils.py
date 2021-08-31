#!/usr/bin/python

import json
import requests

import logging
def check_url_available(urls, timeout=10):
    try:
        for url in urls:
            logging.info(url)
            _ = requests.get(url, timeout=timeout)
        return True
    except requests.ConnectionError:
        return False

def remove_duplicate_records(list_of_dicts):
  list_of_strings = [json.dumps(d, sort_keys=True) for d in list_of_dicts]  
  list_of_strings = set(list_of_strings)  
  return [json.loads(s) for s in list_of_strings]
