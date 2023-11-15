import json
import time

import requests
import backoff
import singer
from singer import metrics

LOGGER = singer.get_logger()


class RateLimitException(Exception):
    pass


class MetricsRateLimitException(Exception):
    pass


class Client(object):
    BASE_URL = 'https://api2.frontapp.com'

    def __init__(self, config):
        self.token = 'Bearer ' + config.get('token')
        self.session = requests.Session()

        self.calls_remaining = None
        self.limit_reset = None

    def url(self, path):
        return self.BASE_URL + path

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, method, url, **kwargs):
        if self.calls_remaining is not None and self.calls_remaining == 0:
            wait = self.limit_reset - int(time.monotonic())
            if 0 < wait <= 300:
                time.sleep(wait)

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        if self.token:
            kwargs['headers']['Authorization'] = self.token

        kwargs['headers']['Content-Type'] = 'application/json'

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
            with metrics.http_request_timer(endpoint) as timer:
                response = requests.request(method, url, **kwargs)
                timer.tags[metrics.Tag.http_status_code] = response.status_code


        else:
            response = requests.request(method, url, **kwargs)

        self.calls_remaining = int(response.headers['X-Ratelimit-Remaining'])
        self.limit_reset = int(float(response.headers['X-Ratelimit-Reset']))

        if response.status_code in [429, 503]:
            raise RateLimitException(response.text)
        if response.status_code == 423:
            raise MetricsRateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.error('{} - {}'.format(response.status_code, response.text))
            raise

        return response

    def get_report_metrics(self, url, **kwargs):
        response = self.request('get', url, **kwargs)
        return response.json().get('metrics', [])

    def create_report(self, path, data, **kwargs):
        url = self.url(path)
        kwargs['data'] = json.dumps(data)
        response = self.request('post', url, **kwargs)
        if response.json().get('_links', {}).get('self'):
            return response.json()['_links']['self']

        return {}

    def list_metrics(self, path, **kwargs):
        url = self.url(path)
        response = self.request('get', url, **kwargs)
        return response.json().get('_results', [])
