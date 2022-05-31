import os, requests, time, xmltodict, datetime, ndjson, random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk, streaming_bulk
from faker import Faker
import pytest


container_host = 'localhost'
elastic_user = 'elastic'
elastic_password = 'elastic_password'
es1_url = f'https://{container_host}:9200'

def get_es_instance():
    es = Elasticsearch(es1_url,
                       basic_auth=('elastic',
                                   elastic_password),
                       verify_certs=False)
    return es

def tear_down_tests(es):
    try:
        es.indices.delete(index='acme_hr')
        es.indices.delete(index='acme_hr_internal')
        es.ingest.delete_pipeline(id='acme_enrichment')
        return True
    except Exception as e:
        return e

def skills_list(faker):
    path_list = [
        "technical_drawing",
        "programming",
        "photography",
        "linguistics",
        "psychology",
        "chemistry",
        "medical_research",
        "medic",
        "sports",
        "martial arts",
        "photographic memory",
        "empathic",
        "polygraph",
        "forensics",
        "land survey",
        "aeronautics",
        "pilot",
        "mechanic",
        "trader",
        "historian",
        "veterenarian",
        "agriculture",
    ]
    personal_list = faker.random_elements(path_list, unique=True, length=random.choice(range(1,4)))
    output = []
    for item in personal_list:
        doc = {'skill_name':item, 'skill_level':random.choice(range(100))}
        output.append(doc)
    return output

def data_generator(document_count):
    faker = Faker()
    documents = []
    for i in range(document_count):
        document = {}
        document['@timestamp'] = datetime.datetime.now().isoformat()
        document['first_name'] = faker.first_name()
        document['last_name'] = faker.last_name()
        document['middle_name'] = faker.random_element([faker.first_name(), faker.last_name()])
        document['telephone_number'] = faker.phone_number()
        document['email'] = faker.email()
        document['employee-id'] = faker.uuid4()
        document['bio'] = faker.text(max_nb_chars=200)
        document['ip_address'] = faker.ipv4_public()
        document['hired_date'] = faker.date()
        document['paid_amount'] = random.choice(range(10000,1000000))
        document['days_in_service'] = (datetime.datetime.now() - datetime.datetime.strptime(document['hired_date'], '%Y-%m-%d')).days
        document['skills'] = skills_list(faker)
        documents.append(document)
    return documents

@pytest.fixture
def file_path():
    return '/opt/interest/bookwork/elasticsearch/data/recipes.json'
    
@pytest.fixture
def es():
    return get_es_instance()

@pytest.fixture
def data_set():
    return data_generator(document_count=1000)

@pytest.fixture
def mapping():
    return {
        "mappings": {
            "properties" : {
                "age" : {
                  "type" : "integer"
                },
                "@timestamp" : {
                  "type" : "date"
                },
                "email" : {
                  "type" : "keyword",
                  "fields": {
                      "raw": { 
                        "type":  "text"
                      }
                    }   
                },
                "first_name": {
                    "type": "keyword",
                    "copy_to": "full_name" 
                },
                "last_name": {
                    "type": "keyword",
                    "copy_to": "full_name" 
                },
                "full_name": {
                    "type": "keyword",
                    "fields": {
                      "raw": { 
                        "type": "text"
                      }
                    }
                },
                "employee-id" : {
                  "type" : "keyword",
                  "index" : False
                },
                "telephone_number" : {
                  "type" : "keyword",
                  "index" : False
                }
            },
            "runtime": {
                "day_of_the_week": {
                    "type": "keyword",
                    "script": {
                        "source": "emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
                    }
                },
                "enterprise_compensation_ratio": {
                    "type": "double",
                    "script": {
                        "source": "emit(doc['paid_amount'].value / doc['days_in_service'].value)"
                    }
                }
            },
            "date_detection": True,
            "numeric_detection": True,
            "dynamic_date_formats": ["yyyy-MM-dd"],
            "dynamic_templates": [
                {
                    "strings_as_ip": {
                        "match_mapping_type": "string",
                        "match": "ip*",
                        "runtime": {
                            "type": "ip"
                        }
                    }
                },
                {
                    "names_as_keywords": {
                        "match_mapping_type": "string",
                        "match":   "^*name",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                },
                {
                    "named_analyzers": {
                        "match_mapping_type": "string",
                        "match": "bio",
                        "mapping": {
                            "type": "text",
                            "analyzer": "english"
                        }
                    }
                }
            ]
        }
    }

@pytest.fixture
def multi_field_query():
    return {
      "query": { 
        "bool": { 
          "must": [
            { "match": { "email.raw":"example.com" }}
          ],
        }
      },
      "sort": {
          "email": "asc"
      }
    }

@pytest.fixture
def reindex_api_document():
    pass

@pytest.fixture
def update_by_query():
    return {
        "script": {
            "source": "ctx._source['enterprise_compensation_ratio'] = ctx._source['paid_amount']/ctx._source['days_in_service']"
        }
    }

@pytest.fixture
def update_by_query_verification():
    return {
    "query": {
        "range": {
          "enterprise_compensation_ratio": {
            "gte": 250
          }
        }
      },
    "script": {
        "source": "ctx._source['leader'] = true",
        "lang": "painless"
        }
    }

@pytest.fixture
def painless_ingest_pipeline():
    return {
      "pipeline": {
        "processors": [
          {
            "fingerprint": {
              "fields": [
                "bio"
              ]
            }
          },
          {
            "geoip": {
              "field": "ip_address"
            }
          }
        ]
      }
    }


class TestDataProcessing:
    
    def test_es_is_alive(self, es):
        assert len(es.cat.nodes(format='json')) > 0

    def test_apply_mapping(self, es, mapping):
        if es.indices.exists(index='acme_hr'):
            es.indices.delete(index='acme_hr')
        assert es.indices.create(index='acme_hr', mappings=mapping['mappings'])['acknowledged'] == True
    
    def test_load_data(self, es, data_set):
        new_docs = []
        for doc in data_set:
            new = {}
            new['_source'] = doc
            new['_id'] = doc['employee-id']
            new['_index'] = 'acme_hr'
            new_docs.append(new)
        bulk(es, new_docs)
        time.sleep(2)
        assert int(es.cat.count(index='acme_hr', format='json')[0]['count']) >= 1000

    def test_apply_analyzer(self, es):
        assert  es.indices.get_mapping(index='acme_hr')['acme_hr']['mappings']['properties']['bio']['analyzer'] == 'english'
    
    def test_apply_multi_field_mapping(self, es, multi_field_query):
        assert int(es.search(index='acme_hr', query=multi_field_query['query'], sort=multi_field_query['sort'], source=["email"])['hits']['total']['value']) > 0
    
    def test_reindex_api(self, es, reindex_api_document):
        assert es.reindex(dest={'index':'acme_hr_internal'}, source={'index':'acme_hr'})['timed_out'] == False
        
    def test_update_by_query(self, es, update_by_query, update_by_query_verification):
        assert es.update_by_query(index='acme_hr', script=update_by_query['script'])['updated'] > 0
        time.sleep(1)
        assert es.update_by_query(index='acme_hr', 
                                  script=update_by_query_verification['script'],
                                 query=update_by_query_verification['query'])['updated'] > 0
    
    def test_painless_ingest_pipeline(self, es, painless_ingest_pipeline):
        time.sleep(1)
        assert es.ingest.put_pipeline(description='acme_enrichment', 
                       processors=painless_ingest_pipeline['pipeline']['processors'],
                      id='acme_enrichment')['acknowledged'] == True
        assert es.update_by_query(pipeline='acme_enrichment', index='acme_hr', conflicts='proceed')['updated'] > 0
    
    
    def test_apply_nested_array_index_template(self, es):
        assert type(es.search(size=1, index='acme_hr')['hits']['hits'][0]['_source']['skills']) == list
        assert type(es.search(size=1, index='acme_hr')['hits']['hits'][0]['_source']['skills'][0]) == dict
    
    def test_tear_down(self, es):
        #assert False
        assert tear_down_tests(es) == True
            
