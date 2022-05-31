import os, time
from elasticsearch import Elasticsearch
import pytest
import warnings
warnings.filterwarnings("ignore")

container_host = 'localhost'
elastic_user = 'elastic'
elastic_password = 'elastic_password'
es1_url = f'https://{container_host}:9200'

def get_es_instance():
    es = Elasticsearch(es1_url, 
                       basic_auth=(elastic_user, elastic_password), 
                       verify_certs=False)
    return es

def tear_down_tests(es):
    try:
        es.ml.close_job(job_id='revenue_over_users_api')
        es.ml.delete_datafeed(datafeed_id='datafeed-revenue_over_users_api')
        es.ml.delete_job(job_id='revenue_over_users_api')
        return True
    except Exception as e:
        return e

@pytest.fixture
def es():
    return get_es_instance()

@pytest.fixture
def anomaly_detection():
    """From the Machine Learning with the ELastic Stack Book, Chapter 3"""
    return {
        "job_id": "revenue_over_users_api",
        "analysis_config": {
            "bucket_span": "15m",
            "detectors": [
                {
                    "detector_description": "high_sum(taxful_total_price)over customer_full_name.keyword",
                    "function": "high_sum",
                    "field_name": "taxful_total_price",
                    "over_field_name": "customer_full_name.keyword"
                }
            ],
                "influencers": [
                    "customer_full_name.keyword"
                ]
        },
            "data_description":{
                "time_field":"order_date",
                "time_format":"epoch_ms"
            }
        }

@pytest.fixture
def anomaly_detection_data_feed():
    return {
      "datafeed_id": "datafeed-revenue_over_users_api",
      "job_id": "revenue_over_users_api",
      "query_delay": "60s",
      "indices": [
        "kibana_sample_data_ecommerce"
      ],
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            }
          ]
        }
      },
      "scroll_size": 1000,
      "chunking_config": {
        "mode": "auto"
      }
    }

@pytest.fixture
def forecast_job():
    return{
      "job_id": "sample_web_logs_destination_prediction_job",
      "description": "",
      "groups": [],
      "analysis_config": {
        "bucket_span": "15m",
        "detectors": [
          {
            "function": "count",
            "partition_field_name": "geo.dest"
          }
        ],
        "influencers": [
          "geo.dest"
        ]
      },
      "data_description": {
        "time_field": "timestamp"
      },
      "analysis_limits": {
        "model_memory_limit": "16MB"
      },
      "model_plot_config": {
        "enabled": False,
        "annotations_enabled": True
      }
    }

@pytest.fixture
def data_feed():
    return {
      "indices": [
        "kibana_sample_data_logs"
      ],
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            }
          ]
        }
      },
      "runtime_mappings": {
        "hour_of_day": {
          "type": "long",
          "script": {
            "source": "emit(doc['timestamp'].value.getHour());"
          }
        }
      },
      "job_id": "sample_web_logs_destination_prediction_job",
      "datafeed_id": "sample_web_logs_destination_prediction_datafeed"
    }

class TestLearning:
    @pytest.mark.learning
    def test_ml_available(self, es):
        assert 'defaults' in es.ml.info()
        
    @pytest.mark.learning
    def test_ml_nodes_available(self, es):
        assert es.ml.get_memory_stats()['_nodes']['total'] >= 1
    
    @pytest.mark.learning
    def test_anomaly_detection_job_creation(self, es, anomaly_detection):
        assert es.ml.put_job(job_id=anomaly_detection['job_id'],
                  analysis_config=anomaly_detection['analysis_config'],
                  data_description=anomaly_detection['data_description']
                 )['job_id'] == 'revenue_over_users_api'
    
    @pytest.mark.learning
    def test_anomaly_detection_data_feed_creation(self, es, anomaly_detection_data_feed):
        assert es.ml.put_datafeed(datafeed_id=anomaly_detection_data_feed['datafeed_id'],
                   job_id=anomaly_detection_data_feed['job_id'],
                   query_delay=anomaly_detection_data_feed['query_delay'],
                   indices=anomaly_detection_data_feed['indices'],
                   query=anomaly_detection_data_feed['query'],
                   scroll_size=anomaly_detection_data_feed["scroll_size"],
                   chunking_config=anomaly_detection_data_feed["chunking_config"])['datafeed_id'] == 'datafeed-revenue_over_users_api'
    
    @pytest.mark.learning
    def test_anomaly_detection_buckets_empty(self, es):
        assert es.ml.get_overall_buckets(job_id='revenue_over_users_api')['count'] == 0
    
    @pytest.mark.learning
    def test_anomaly_detection_job_opened(self, es):
        assert es.ml.open_job(job_id='revenue_over_users_api')['opened'] == True
        assert es.ml.get_job_stats(job_id='revenue_over_users_api')['jobs'][0]['state'] == 'opened'
    
    @pytest.mark.learning
    def test_anomaly_detection_feed_started(self, es):
        assert es.ml.start_datafeed(datafeed_id='datafeed-revenue_over_users_api')['started'] == True
    
    @pytest.mark.learning
    def test_anomaly_detection_buckets_created(self, es):
        time.sleep(10)
        test = es.ml.get_overall_buckets(job_id='revenue_over_users_api')
        assert len(test['overall_buckets']) > 0
    
    @pytest.mark.learning
    def test_anomaly_detection_is_running_real_time(self, es):
        assert es.ml.get_datafeed_stats(datafeed_id='datafeed-revenue_over_users_api')['datafeeds'][0]['running_state']['real_time_running'] == True
    
    @pytest.mark.learning
    def test_anomaly_detection_feed_stopped(self, es):
        assert es.ml.stop_datafeed(datafeed_id='datafeed-revenue_over_users_api')['stopped'] == True
    
    @pytest.mark.learning
    def test_job_is_accessible(self, es):
        assert 'revenue_over_users_api' in [x['job_id'] for x in es.ml.get_jobs()['jobs']]
    
    @pytest.mark.learning
    def test_anomaly_detection_data_availability(self, es):
        assert es.ml.get_records(job_id='revenue_over_users_api')['count'] > 0
        assert es.ml.get_records(job_id='revenue_over_users_api')['records'][0]['customer_full_name.keyword'] == ['Wagdi Shaw']
    
    @pytest.mark.forecasting
    def test_forecasting(self, es, forecast_job, data_feed):
        assert es.ml.put_job(job_id=forecast_job['job_id'],
              analysis_config=forecast_job['analysis_config'],
              data_description=forecast_job['data_description']
             )['job_id'] == 'sample_web_logs_destination_prediction_job'
        assert es.ml.put_datafeed(datafeed_id=data_feed['datafeed_id'],
                   job_id=data_feed['job_id'],
                   indices=data_feed['indices'],
                   query=data_feed['query'])['datafeed_id'] == 'sample_web_logs_destination_prediction_datafeed'
        assert es.ml.open_job(job_id='sample_web_logs_destination_prediction_job')['opened'] == True
        assert 'sample_web_logs_destination_prediction_datafeed' in [ x['datafeed_id'] for x in es.ml.get_datafeeds()['datafeeds']]
        assert es.ml.start_datafeed(datafeed_id='sample_web_logs_destination_prediction_datafeed')['started'] == True
        time.sleep(60)
        forecast_result = es.ml.forecast(job_id='sample_web_logs_destination_prediction_job', duration='1d')
        assert 'forecast_id' in forecast_result
        forecast_query = {
          "query": {
            "bool" :{
              "filter": [
                { 
                  "query_string":{
                    "query": "result_type:model_forecast",
                    "analyze_wildcard": True
                  }
                },
                {"term": { "job_id": "sample_web_logs_destination_prediction_job"}},
                {"term": {
                  "forecast_id": f"{forecast_result['forecast_id']}"
                }},
                {"term": { "partition_field_value": "US"}}
                ]
            }
          }
        }
        time.sleep(60)
        US_forecast = es.search(query=forecast_query['query'], index='.ml-anomalies*')
        assert US_forecast['hits']['total']['value'] > 0
        assert es.ml.stop_datafeed(datafeed_id='sample_web_logs_destination_prediction_datafeed')['stopped'] == True
        assert es.ml.close_job(job_id='sample_web_logs_destination_prediction_job')['closed'] == True
        assert es.ml.delete_forecast(forecast_id=forecast_result['forecast_id'], job_id='sample_web_logs_destination_prediction_job')['acknowledged'] == True

    @pytest.mark.forecasting
    def test_tear_down_forecast(self, es):
        try:
            time.sleep(5)
            es.ml.close_job(job_id='sample_web_logs_destination_prediction_job')
            es.ml.delete_forecast(forecast_id='_all', job_id='sample_web_logs_destination_prediction_job')
            es.ml.delete_job(job_id='sample_web_logs_destination_prediction_job')
            assert True
        except Exception as e:
            assert False
            
    @pytest.mark.learning
    def test_tear_down(self, es):
        #assert False
        assert tear_down_tests(es) == True
