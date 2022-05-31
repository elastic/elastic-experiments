import os, requests, xmltodict
from elasticsearch import Elasticsearch
import ndjson
import pytest

container_host = 'localhost'
elastic_user = 'elastic'
elastic_password = 'elastic_playground'
es1_url = f'https://{container_host}:9200'
recipe_file = '/Users/jesse.bacon/elasticsearch/data/recipes.json'

def get_es_instance():
    es = Elasticsearch(es1_url,
                       basic_auth=('elastic',
                                   elastic_password),
                       verify_certs=False)
    return es

def tear_down_tests(es):
    try:
        assert es.indices.delete_alias(index='recipes',name='cookbook')['acknowledged'] == True
        assert es.delete_script(id='recipe_instructions_keyword')['acknowledged'] == True
        assert es.indices.delete(index='recipes')['acknowledged'] == True
        return True
    except Exception as e:
        return False

@pytest.fixture
def file_path():
    return recipe_file
    
@pytest.fixture
def es():
    return get_es_instance()

@pytest.fixture
def data_set(file_path):
    with open(file_path, 'r') as f:
        data = ndjson.loads(f.read())
    return data

@pytest.fixture
def search_with_highligted_results():
    return {
      "query": {
        "match": { "title": "Simple Syrup" }
      },
      "highlight": {
        "fields": {
          "title": {}
        }
      }
    }

@pytest.fixture
def query_with_sorting_requirements():
    return {
      "size": 3,
      "query": {
        "bool": {
          "must": [
            {
              "match": {
                "ingredients": "cabbage"
              }
            }
          ]
        }
      }, 
      "sort": [
        {
          "rating.ratingValue.keyword": {
            "order": "asc"
          }
        }
      ]
    }

@pytest.fixture
def query_with_paginated_results():
    return {
        "from": 4,
        "size": 3,
        "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "ingredients": "cabbage"
                  }
                }
              ]
            }
          }, 
          "sort": [
            {
              "rating.ratingValue.keyword": {
                "order": "asc"
              }
            }
          ]
        }

@pytest.fixture
def index_alias_document():
    return {
      "actions": [
        {
          "add": {
            "index": "recipes*",
            "alias": "cookbook"
          }
        }
      ]
    }

@pytest.fixture
def search_template():
    return {
      "script": {
        "lang": "mustache",
        "source": {
          "query": {
            "match": {
              "instructions": "{{query_string}}"
            }
          }
        },
        "params": {
          "query_string": "My query string"
        }
      }
    }

class TestSearchApplications:

    def test_es_is_alive(self, es):
        assert len(es.cat.nodes(format='json')) > 0
    
    def test_setup(self, es, data_set):
        if es.indices.exists(index='recipes'):
            assert True
            #es.indices.delete(index='recipes')
        else:
            try:
                for document in data_set:
                    es.index(index='recipes', document=document)
            except:
                assert False
    
    def test_highlighted_search_results(self, es, search_with_highligted_results):
        assert '<em>' in es.search(index='recipes', query=search_with_highligted_results['query'], 
          highlight=search_with_highligted_results['highlight'])['hits']['hits'][0]['highlight']['title'][0]
        
    def test_sorting_query_with_rquirements(self, es, query_with_sorting_requirements):
        assert len(es.search(index='recipes', 
              query=query_with_sorting_requirements['query'],
              sort=query_with_sorting_requirements['sort'],
              size=query_with_sorting_requirements['size'])['hits']['hits']) ==3 
    
    def test_query_with_paginated_results(self, es, query_with_paginated_results):
        assert len(es.search(index='recipes', 
              query=query_with_paginated_results['query'],
              sort=query_with_paginated_results['sort'],
              size=query_with_paginated_results['size'],
              from_=query_with_paginated_results['from'])['hits']['hits']) ==3 
        
    def test_define_an_index_alias(self, es, index_alias_document):
        for action in index_alias_document['actions']:
            assert es.indices.put_alias(index=action['add']['index'],
                                   name=action['add']['alias'])
        
    def test_define_search_template(self, es, search_template):
        assert es.put_script(id='recipe_instructions_keyword',
              script=search_template['script'])['acknowledged'] == True
        
    def test_use_search_template(self, es):
        template = es.render_search_template(id='recipe_instructions_keyword', params={'query_string':'egg beater'})
        assert 'template_output' in template.keys()
        assert es.search(index='recipes', query=template['template_output']['query'])['hits']['total']['value'] == 1424
        
    def test_tear_down(self, es):
        #assert True
        assert tear_down_tests(es) == True
    
