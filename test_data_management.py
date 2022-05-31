import requests, time, xmltodict, datetime
from elasticsearch import Elasticsearch
import pytest

container_host = 'localhost'
elastic_user = 'elastic'
elastic_password = 'elastic_playground'
es1_url = f'https://{container_host}:9200'

def get_es_instance():
    es = Elasticsearch(es1_url,
                       basic_auth=('elastic',
                                   elastic_password),
                       verify_certs=False)
    return es

@pytest.fixture
def es():
    return get_es_instance()

@pytest.fixture
def dynamic_index_settings():
    return {
      "persistent": {
        "action.auto_create_index": "true"
      }
    }

@pytest.fixture
def static_settings():
    return { 
        'index': {
            # default 1024
            "number_of_shards":5,
            'number_of_routing_shards':30,
            'codec':'best_compression',
            'routing_partition_size':1,
            'soft_deletes': {'enabled':True, 'retention_lease':{'period':'12h'}},
            'load_fixed_bitset_filters_eagerly':True,
            'shard':{'check_on_startup':False}
        }
    }

@pytest.fixture
def dynamic_settings():
    return {
        'index':{
            "number_of_shards":5,
            'number_of_routing_shards':30,
            'codec':'best_compression',
            'routing_partition_size':1,
            'soft_deletes': {'enabled':True, 'retention_lease':{'period':'12h'}},
            'load_fixed_bitset_filters_eagerly':True,
            'shard':{'check_on_startup':False}
        }
    }

@pytest.fixture
def test_1_mappings():
    return {
        "properties": {
            "city": {
                "type": "text",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "state": {
                "type": "text",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "address":{
                "type": "text"
            },
            "population": {
                "type": 'long'
            },
            "pop": {
                "type": "alias",
                "path": "population"
            }
        }
    }

@pytest.fixture
def component_template_1():
    return {
      "template": {
        "mappings": {
          "properties": {
            "@timestamp": {
              "type": "date"
            }
          }
        }
      }
    }

@pytest.fixture
def component_template_2():
    return {
  "template": {
    "mappings": {
      "runtime": { 
        "day_of_week": {
          "type": "keyword",
          "script": {
            "source": "emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
          }
        }
      }
    }
  }
}

@pytest.fixture
def composed_of_template():
    return {
      "index_patterns": ["my-exam*"],
      "template": {
        "settings": {
          "number_of_shards": 1
        },
        "mappings": {
          "_source": {
            "enabled": True
          },
          "properties": {
            "host_name": {
              "type": "keyword"
            },
            "created_at": {
              "type": "date",
              "format": "EEE MMM dd HH:mm:ss Z yyyy"
            }
          }
        },
        "aliases": {
          "my-exam": { }
        }
      },
      "priority": 500,
      "composed_of": ["component_template1", "component_template2"], 
      "version": 3,
      "_meta": {
        "description": "my exam component template composition"
      }
    }

@pytest.fixture
def standard_template_mappings():
     return {
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "source.ip": {
          "type": "ip"
        },
        "destination.ip": {
          "type": "ip"
        },
        "event.action": {
          "type": "keyword"
        },
        "user.name": {
          "type": "keyword"
        },
        "client.bytes": {
          "type": "double"
        }
      }
    }

@pytest.fixture
def standartd_template_test_document():
    return {
        'source.ip':'192.168.1.1',
        'destination.ip':'192.168.1.101',
        'event.action':'ACK',
        'user.name':'Jesse',
        'client.bytes':32
    }

@pytest.fixture
def dynamic_template_mappings():
    return {  
        "mappings": {
            "dynamic_templates": [
              {
                "strings_as_keywords": {
                  "match_mapping_type": "string",
                  "mapping": {
                    "type": "text",
                    "norms": False,
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            ]
          }
        }

@pytest.fixture
def dynamic_template_test_document():
    return {
      '@timestamp': datetime.datetime.now(),
      "english": "Some English text",
      "count":   5
    }

@pytest.fixture
def index_life_cycle_management_index_template():
    return {
      "index_patterns": ["exam-timeseries-*"], 
      "template": {
        "settings": {
          "number_of_shards": 1,
          "number_of_replicas": 1,
          "index.lifecycle.name": "exam-timeseries-policy", 
          "index.lifecycle.rollover_alias": "timeseries" 
        }
      }
    }

@pytest.fixture
def index_life_cycle_policy():
    return {
      "policy": {
        "_meta": {
          "description": "used for timeseries data",
          "project": {
            "name": "my-exam",
            "department": "research and development"
          }
        },
        "phases": {
          "warm": {
            "min_age": "1d",
            "actions": {
              "forcemerge": {
                "max_num_segments": 1
              }
            }
          },
          "delete": {
            "min_age": "5d",
            "actions": {
              "delete": {}
            }
          }
        }
      }
    }

@pytest.fixture
def timeseries_alias():
    return {
      "aliases": {
        "exam-timeseries": {
          "is_write_index": True
        }
      }
    }

@pytest.fixture
def data_stream_mappings():
    return {
      "template": {
        "mappings": {
          "properties": {
            "@timestamp": {
              "type": "date",
            },
            "message": {
              "type": "wildcard"
            }
          }
        }
      },
      "_meta": {
        "description": "Mappings for @timestamp and message fields",
        "exam-custom-meta-field": "More arbitrary metadata"
      }
    }

@pytest.fixture
def data_stream_settings():
    return {
      "template": {
        "settings": {
          "index.lifecycle.name": "exam-lifecycle-policy"
        }
      },
      "_meta": {
        "description": "Settings for ILM",
        "my-custom-meta-field": "More arbitrary metadata"
      }
    }

@pytest.fixture
def data_stream_template():
    return {
      "index_patterns": ["exam-data-stream*"],
      "data_stream": { },
      "composed_of": [ "exam-mappings", "exam-settings" ],
      "priority": 500,
      "_meta": {
        "description": "Template for my time series data",
        "my-custom-meta-field": "More arbitrary metadata"
      }
    }

@pytest.fixture
def data_stream_documents():
    return [
        { "@timestamp": datetime.datetime.now(), "message": "192.0.2.42 - - [06/May/2099:16:21:15 +0000] \"GET /images/bg.jpg HTTP/1.0\" 200 24736" },
        { "@timestamp": datetime.datetime.now(), "message": "192.0.2.255 - - [06/May/2099:16:25:42 +0000] \"GET /favicon.ico HTTP/1.0\" 200 3638" },
        { "@timestamp": datetime.datetime.now(), "message": "192.0.2.42 - - [06/May/2099:16:21:15 +0000] \"GET /images/bg.jpg HTTP/1.0\" 200 24736"}
    ]


class TestDataManagement:

    def test_index_settings(self, es, dynamic_index_settings):
        assert es.cluster.put_settings(body=dynamic_index_settings)

    def test_static_index_creation(self, es, static_settings, test_1_mappings):
        if  es.indices.exists(index='exam_objective_data_management_static_index').raw == True:
            es.indices.delete(index='exam_objective_data_management_static_index')
        es.indices.create(
            index='exam_objective_data_management_static_index',
            aliases=None,
            mappings=test_1_mappings,
            settings = static_settings,
            timeout = None,
            wait_for_active_shards=1
        )
        assert es.indices.exists(index='exam_objective_data_management_static_index').raw == True
        mappings = es.indices.get_mapping(index='exam_objective_data_management_static_index')
        assert 'keyword' == mappings['exam_objective_data_management_static_index']['mappings']['properties']['city']['fields']['raw']['type']
        
    def test_dynamic_index_creation(self, es, dynamic_settings):
        if  es.indices.exists(index='exam_objective_data_management_dynamic_index').raw == True:
            es.indices.delete(index='exam_objective_data_management_dynamic_index')
        es.indices.create(
            index='exam_objective_data_management_dynamic_index',
            aliases=None,
            mappings=None,
            settings = dynamic_settings,
            timeout = None,
            wait_for_active_shards=1
        )
        assert es.indices.exists(index='exam_objective_data_management_dynamic_index').raw == True
    
    def test_component_template_assembly(self, es, component_template_1, component_template_2, composed_of_template):
        if es.indices.exists_index_template(name='template_1'):
            es.indices.delete_index_template(name='template_1')
        if es.cluster.exists_component_template(name='component_template1'):
            try:
                es.indices.delete_index_template(name='exam_objective_data_management_composed_of_template')
            except:
                pass
            es.cluster.delete_component_template(name='component_template1')
        if es.cluster.exists_component_template(name='component_template2'):
            try:
                es.indices.delete_index_template(name='exam_objective_data_management_composed_of_template')
            except:
                pass
            es.cluster.delete_component_template(name='component_template2')
        es.cluster.put_component_template(name='component_template1', template=component_template_1['template'])
        es.cluster.put_component_template(name='component_template2', template=component_template_2['template'])
        assert es.cluster.exists_component_template(name='component_template1')
        assert es.cluster.exists_component_template(name='component_template2')
        if es.indices.exists_index_template(name='exam_objective_data_management_composed_of_template'):
            es.indices.delete_index_template(name='exam_objective_data_management_composed_of_template')
        assert es.indices.put_index_template(name='exam_objective_data_management_composed_of_template',
                                      index_patterns=composed_of_template['index_patterns'],
                                      template=composed_of_template['template'],
                                      priority=500,
                                      composed_of=['component_template1', 'component_template2'],
                                      version=3,
                                      meta=composed_of_template['_meta'])['acknowledged']
        
    
    def test_standard_index_template(self, es, standard_template_mappings, standartd_template_test_document):
        if es.indices.exists_index_template(name='standard_template_1'):
             es.indices.delete_index_template(name='standard_template_1')
        es.indices.put_template(name="standard_template_1",index_patterns=['my-standard-exam-ip-logs'],
                                mappings=standard_template_mappings, 
                                settings={"number_of_shards":2})
        assert es.index(index='my-standard-exam-ip-logs-0001', document=standartd_template_test_document)['result'] == 'created'
        assert es.indices.get_mapping(index='my-standard-exam-ip-logs-0001')['my-standard-exam-ip-logs-0001']['mappings']['properties']['source']['properties']['ip']['type'] == 'text'
    
    def test_dynamic_template(self, es, dynamic_template_mappings, dynamic_template_test_document):
        if es.cluster.exists_component_template(name='text_as_keyword') == True:
            if es.indices.exists_index_template(name='data_management_dynamic_component_template'):
                es.indices.delete_index_template(name='data_management_dynamic_component_template')
            #es.cluster.delete_component_template(name='text_as_keyword')
        assert es.cluster.put_component_template(name='text_as_keyword', template=dynamic_template_mappings)['acknowledged'] == True
        if es.indices.exists_index_template(name='data_management_dynamic_component_template'):
            es.indices.delete_index_template(name='data_management_dynamic_component_template')
        assert es.indices.put_index_template(name='data_management_dynamic_component_template',index_patterns=['my-exam-text-keyword-index-*'],
                              template={},composed_of=['text_as_keyword'],version=3)['acknowledged'] == True
        assert es.index(document=dynamic_template_test_document, index='my-exam-text-keyword-index-0001')['result'] == 'created'
        assert es.indices.get_mapping(index='my-exam-text-keyword-index-0001')['my-exam-text-keyword-index-0001']['mappings']['properties']['english']['fields']['keyword']['type'] == 'keyword'
        
    def test_ilm_policy(self, es, index_life_cycle_policy, index_life_cycle_management_index_template, timeseries_alias):
        assert es.ilm.put_lifecycle(name='exam-timeseries-policy', policy=index_life_cycle_policy['policy'])['acknowledged'] == True
        assert es.indices.put_template(name='my-exam-timeseries', index_patterns=index_life_cycle_management_index_template['index_patterns'], 
                        settings=index_life_cycle_management_index_template['template']['settings'])['acknowledged'] == True
        if es.indices.exists(index='exam-timeseries-000001'):
            assert es.indices.delete(index='exam-timeseries-000001')['acknowledged'] == True
        assert es.indices.create(index='exam-timeseries-000001', aliases=timeseries_alias['aliases'])['acknowledged'] == True
        assert es.ilm.explain_lifecycle(index='exam-timeseries-000001')['indices']['exam-timeseries-000001']['lifecycle_date_millis'] > 0
        
    def test_data_stream(self, es, data_stream_mappings, data_stream_settings, data_stream_template, data_stream_documents):
        assert es.cluster.put_component_template(name='exam-mappings',template=data_stream_mappings['template'])['acknowledged'] == True
        assert es.cluster.put_component_template(name='exam-settings',template=data_stream_settings['template'])['acknowledged'] == True
        assert es.indices.put_index_template(name='exam-index-template',
                                             index_patterns=data_stream_template['index_patterns'],
                                             priority=500,
                                             data_stream=data_stream_template['data_stream'],
                                             meta=data_stream_template['_meta'],
                                             composed_of=data_stream_template['composed_of'],
                                            )['acknowledged'] == True
        for doc in data_stream_documents:
            es.create(index='exam-data-stream', document=doc, id=datetime.datetime.now())
        time.sleep(3)
        assert es.indices.get_data_stream(name='exam-data-stream')['data_streams'][0]['status'] in ['GREEN', 'YELLOW']
    
    def test_tear_down(self, es):
        assert es.indices.delete(index='exam_objective_data_management_static_index')['acknowledged'] == True
        assert es.indices.delete(index='exam_objective_data_management_dynamic_index')['acknowledged'] == True
        assert es.indices.delete_index_template(name='exam_objective_data_management_composed_of_template')['acknowledged'] == True
        assert es.cluster.delete_component_template(name='component_template1')['acknowledged'] == True
        assert es.cluster.delete_component_template(name='component_template2')['acknowledged'] == True
        assert es.indices.delete_index_template(name='data_management_dynamic_component_template')['acknowledged'] == True
        assert es.indices.delete_template(name='my-exam-timeseries')
        assert es.indices.delete(index='exam-timeseries-000001')['acknowledged'] == True
        assert es.ilm.delete_lifecycle(name='exam-timeseries-policy')
        assert es.indices.delete_data_stream(name='exam-data-stream')['acknowledged'] == True
