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
        self.metric = config.get('metric')
        self.session = requests.Session()

        self.calls_remaining = None
        self.limit_reset = None


    def url(self, path):
        return self.BASE_URL + path

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, method, path, **kwargs):
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
                #print(', '.join(['{}={!r}'.format(k, v) for k, v in kwargs.items()]))
                response = requests.request(method, self.url(path), **kwargs)
                #print('final url=',response.url)
                timer.tags[metrics.Tag.http_status_code] = response.status_code

                # frontapp's API takes an initial request then needs 1 sec to produce the report
                #so here we just run the request again
                #print('sleeping 2')
                time.sleep(2)
                response = requests.request(method, self.url(path), **kwargs)
                #print('final2 url=',response.url)


        else:
            response = requests.request(method, self.url(path), **kwargs)
            #print('final3 url=',response.url)
            #print('sleeping2 2')
            time.sleep(2)
            response = requests.request(method, self.url(path), **kwargs)
            #print('final4 url=',response.url)

        self.calls_remaining = int(response.headers['X-Ratelimit-Remaining'])
        self.limit_reset = int(float(response.headers['X-Ratelimit-Reset']))

        if response.status_code in [429, 503]:
            raise RateLimitException()
        if response.status_code == 423:
            raise MetricsRateLimitException()
        try:
            response.raise_for_status()
        except:
            LOGGER.error('{} - {}'.format(response.status_code, response.text))
            raise
        #print('get data=',response.json()['metrics'][0]['rows'])
        return response.json()['metrics'][0]['rows']

    def get(self, path, **kwargs):
        return self.request('get', path, **kwargs)

    def post(self, path, data, **kwargs):
        kwargs['data'] = json.dumps(data)
        return self.request('post', path, **kwargs)
