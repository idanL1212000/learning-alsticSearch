import time
from pprint import pprint

from elasticsearch import Elasticsearch

from config import ES_CLIENT_URL


def get_es_client(max_retries: int = 5, sleep_time: int = 5) -> Elasticsearch:
    for _ in range(max_retries):
        try:
            es_client = Elasticsearch(ES_CLIENT_URL)
            client_info = es_client.info()
            pprint("Connected To Elasticsearch!")
            return es_client
        except Exception:
            pprint("Could not connect to Elasticsearch, retrying...")
            time.sleep(sleep_time)
    raise ConnectionError(f"Failed to connect to Elasticsearch after {max_retries} attempts.")