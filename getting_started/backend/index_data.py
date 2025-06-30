import json
from pprint import pprint
from typing import List, Any

from elastic_transport import ObjectApiResponse
from tqdm import tqdm
from elasticsearch import Elasticsearch

from config import INDEX_NAME
from utils import get_es_client


def index_data(documents: List[dict]):
    es_client = get_es_client(max_retries=5,sleep_time=5)
    _ = _create_index(es_client)
    _ = _insert_documents(es_client, documents)
    pprint(f'Indexed {len(documents)} docs into Elasticsearch index "{INDEX_NAME}"')

def _create_index(es_client: Elasticsearch) -> dict:
    es_client.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
    return es_client.indices.create(index=INDEX_NAME)

def _insert_documents(es_client: Elasticsearch, documents: List[dict]) -> ObjectApiResponse[Any]:
    operations = []
    for doc in tqdm(documents, total=len(documents), desc='Indexing documents'):
        operations.append({'index': {'_index': INDEX_NAME}})
        operations.append(doc)
    return es_client.bulk(operations=operations)

if __name__ == '__main__':
    with open('../../data/apod.json') as f:
        documents = json.load(f)
    index_data(documents=documents)