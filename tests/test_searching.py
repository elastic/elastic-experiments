import requests, time, xmltodict
from elasticsearch import Elasticsearch
import logging, ndjson
import pytest

es_host = 'localhost'
es1_url = f'https://{es_host}:9200'
es2_url = f'https://{es_host}:9201'
es_password = 'elastic_password'
movies_file = 'data/movies.json'

def get_es_instance():
    es = Elasticsearch(es1_url, 
                       basic_auth=('elastic',es_password), 
                       verify_certs=False)
    return es

def get_es2_instance():
    es2 = Elasticsearch(es2_url, 
                       basic_auth=('elastic',es_password), 
                       verify_certs=False)
    return es2

def tear_down_data_set(es):
    try:
        es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : [f"{es_host}:9301" ]}}}}, 'transient': {}})
        return True
    except Exception as e:
        return False
@pytest.fixture
def data_path():
    return movies_file

@pytest.fixture
def es():
    return get_es_instance()

@pytest.fixture
def es2():
    return get_es2_instance()

@pytest.fixture
def data_file(data_path):
    with open(data_path, 'r') as f:
        return ndjson.loads(f.read())

@pytest.fixture
def ingest_pipeline():
    return {
      "description": "Prepares the Mongo DB Movies Data Set for Elasticsearch",
      "processors": [
        {
          "set": {
            "field": "content_from",
            "value": "mongo m312"
          }
        }
      ]
    }

@pytest.fixture
def multi_field_terms_query():
    return {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "countries.keyword": "USA"
              }
            },
            {
              "term": {
                "genres.keyword": "Comedy"
              }
            },
            {
              "range": {
                "year": {
                  "lt": 1920
                }
              }
            }
          ],
          "must": [
            {
              "match": {
                "rated": "UNRATED"
              }
            }
          ]
        }
      }
    }

@pytest.fixture
def asynchronous_search():
    return {
      "size": 0, 
      "query": {
        "exists": {
          "field": "tomatoes"
        }
      },
      "aggs": {
        "average_rating": {
          "avg": { 
            "field": "tomatoes.viewer.rating"  
          }
        }
      }
    }

@pytest.fixture
def metric_aggregation():
    return {
      "_source": ["title", "tomatoes.viewer.rating"], 
      "query": {
        "range": {
          "tomatoes.viewer.rating": {
            "gte": 5
          }
        }
      }
    }

@pytest.fixture
def bucket_histogram():
    return {
      "size":0,
      "aggs": {
        "ratings": {
          "variable_width_histogram": {
            "field": "tomatoes.viewer.rating",
            "buckets": 5
          }
        }
      }
    }

@pytest.fixture
def bucket_sub_aggregation():
    return {
      "size":0,
      "aggs": {
        "ratings": {
          "histogram": {
            "field": "year",
            "interval":5
          },
          "aggs": {
            "combined_awesomeness": {
              "sum": {
                "field": "tomatoes.viewer.rating"
              }
            }
          }
        },
        "awesome_filming_year_start":{
          "max_bucket": {
            "buckets_path": "ratings>combined_awesomeness"
          }
        }
      }
    }



class TestSearchingData:

    def test_create_data_set(self, es, data_file, ingest_pipeline):
        if es.indices.exists(index='movies'):
            es.indices.delete(index='movies')
        assert es.ingest.put_pipeline(id='movies_pipeline', 
                               processors=ingest_pipeline['processors'],
                              description=ingest_pipeline['description'])['acknowledged'] == True
        i = 0
        for doc in data_file:
            doc['old_id'] = doc['_id']
            doc.pop('_id')
            try:
                es.index(index='movies', pipeline='movies_pipeline', document=doc)
                i+=1
            except:
                pass
            if i >= 1000:
                break
        time.sleep(3)
        assert int(es.cat.count(index='movies', format='json')[0]['count']) == 1000
        logging.warning("Initial Dataset populated with 1000 entries")
    
    def test_terms_aggregation(self, es, multi_field_terms_query):
        assert 'The Immigrant' in [x['_source']['title'] for x in es.search(index='movies', query=multi_field_terms_query['query'])['hits']['hits']]
    
    def test_asynchronous_search(self, es, asynchronous_search):
        assert 'is_running' in es.async_search.submit(index='movies', query=asynchronous_search['query'], size=0, aggs=asynchronous_search['aggs'])
    
    def test_metric_aggregation(self, es, metric_aggregation):
        assert es.search(index='movies', source=metric_aggregation['_source'], query=metric_aggregation['query']).raw['hits']['total']['value'] > 0
        
    def test_bucket_histogram(self, es, bucket_histogram):
        assert es.search(index='movies', aggs=bucket_histogram['aggs'])['hits']['total']['value'] == 1000
    
    def test_bucket_sub_aggregation(self, es, bucket_sub_aggregation):
        assert 'awesome_filming_year_start' in es.search(index='movies', aggs=bucket_sub_aggregation['aggs'])['aggregations']
    
    def test_cross_cluster_deploy(self, es, es2):
        assert len(es.cat.nodes(format='json')) == 3
        assert len(es2.cat.nodes(format='json')) == 3
        assert es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : ["localhost:9301" ]}}}}, 'transient': {}})['acknowledged'] == True
    
    def test_cross_cluser_data_load(self, es2):
        assert es2.index(index='ccs_test', document={'test':'success'})['result'] == 'created'
        time.sleep(3)
    
    def test_cross_cluster_search(self, es):
        assert es.search(index='cluster_b:ccs_test')['hits']['hits'][0]['_source']['test'] == 'success'
    
    def test_tear_down_local(self, es):
        assert tear_down_data_set(es) == True
                                           
