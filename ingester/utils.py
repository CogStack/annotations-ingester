#!/usr/bin/python

import requests


def check_url_available(url, timeout=3):
    try:
        _ = requests.get(url, timeout=timeout)
        return True
    except requests.ConnectionError:
        return False
