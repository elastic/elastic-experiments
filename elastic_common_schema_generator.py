import os, requests, zipfile, json, random, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk, streaming_bulk
from faker import Faker
import yaml
from yaml import Loader
from copy import deepcopy
import numpy as np


class ECSRecords:
    
    def __init__(self):
        self.container_host = 'localhost'
        self.elastic_user = 'elastic'
        self.elastic_password = 'elastic_playground'
        self.es1_url = f'https://{self.container_host}:9200'
        self.es = Elasticsearch(self.es1_url, 
                       basic_auth=(self.elastic_user, self.elastic_password), 
                       verify_certs=False)
    

    def create_records(self, base_document, processors, count=100):
        records = []
        for i in range(count):
            document = deepcopy(base_document)
            for processor in processors:
                document = self.handle_document_processor(document, processor)
            records.append(document)
        return records

    def handle_document_processor(self, base_document, processor):
        if processor == 'host':
            return self.host_processor(base_document)
        if processor == 'cve':
            return self.cve_processor(base_document)

    def generate_processor_list(self, object_type, schema):
        code_start = 'fake = Faker()\n'
        processor_list = schema[object_type]['fields']
        for item in processor_list:
            code_start += f'#{item} = fake. \n'
            try:
                example = processor_list[item]['example']
                code_start += f'# {example} \n'
            except Exception as e:
                pass
        return processor_list, code_start

    def pull_schema(self):
        elastic_common_schema_reference_link = 'https://raw.githubusercontent.com/elastic/ecs/main/generated/ecs/ecs_nested.yml'
        schema = requests.get(elastic_common_schema_reference_link).content
        with open('schema.yml', 'w') as f:
            f.write(schema.decode())
        specification = yaml.safe_load(schema.decode())
        return specification

    def host_processor(self, base_document):
        fake = Faker()
        base_document['host.architecture'] = random.choice(['x86_64', 'amd64', 'arm', 'arm64', '386', 'mips64le', 'ppc64le', 's390x']) 
        base_document['host.boot.id'] = fake.uuid4()
        base_document['host.cpu.usage'] = '' 
        base_document['host.disk.read.bytes'] =  ''
        base_document['host.disk.write.bytes'] = ''
        base_document['host.domain'] = random.choice(['thefirm.remote', 'theclient.remote', 'thesite.local']) 
        if base_document['host.domain'] == 'thefirm.remote':
            base_document['host.geo.city_name'] = random.choice(['New York', 'Los Angeles'])
        elif base_document['host.domain'] == 'theclient.remote':
            base_document['host.geo.city_name'] = random.choice(['Providence', 'St. Paul', 'Huntsville'])
        elif base_document['host.domain'] == 'thesite.local':
            base_document['host.geo.city_name'] = random.choice(['Dallas', 'Atlanta', 'Denver'])
        base_document['host.geo.continent_code'] = 'NA' 
        base_document['host.geo.continent_name'] = 'North America'
        base_document['host.geo.country_iso_code'] = 'US'
        base_document['host.geo.country_name'] = 'United States' 
        if base_document['host.geo.city_name'] == 'New York':
            base_document['host.geo.location'] = {'lat':40.75464371951375, 'lon':-73.98552474566057}
        elif base_document['host.geo.city_name'] == 'Los Angeles':
            base_document['host.geo.location'] = {'lat':34.21065042254456, 'lon':-118.46447240169385}
        elif base_document['host.geo.city_name'] == 'Providence':
            base_document['host.geo.location'] = {'lat':41.823355702005315, 'lon':-71.403246780318}
        elif base_document['host.geo.city_name'] == 'St. Paul':
            base_document['host.geo.location'] = {'lat':34.71637491875687, 'lon':-86.63886064890734}
        elif base_document['host.geo.city_name'] == 'Dallas':
            base_document['host.geo.location'] = {'lat':32.78755153564765, 'lon':-96.80654594317316}
        elif base_document['host.geo.city_name'] == 'Atlanta':
            base_document['host.geo.location'] = {'lat':33.77267019013217, 'lon':-84.4088795932221}
        elif base_document['host.geo.city_name'] == 'Denver':
            base_document['host.geo.location'] = {'lat':39.751608692496134, 'lon':-104.94184561922224}
        base_document['host.geo.name'] = '' 
        base_document['host.geo.postal_code'] = '' 
        base_document['host.geo.region_iso_code'] = ''
        base_document['host.geo.region_name'] = ''
        base_document['host.geo.timezone'] = ''
        base_document['host.hostname'] = fake.hostname() + '.' + base_document['host.domain'] 
        base_document['host.id'] = fake.uuid4() 
        #host.ip = Faker. 
        if base_document['host.domain'] == 'thefirm.remote':
            base_document['host.ip'] = fake.ipv4_public()
        elif base_document['host.domain'] == 'theclient.remote':
            base_document['host.ip'] = fake.ipv4_public()
        elif base_document['host.domain'] == 'thesite.local':
            base_document['host.ip'] = fake.ipv4_private()
        base_document['host.mac'] = [fake.mac_address() for x in range(random.choice(range(4))+1)]
        base_document['host.name'] = base_document['host.hostname']
        base_document['host.network.egress.bytes'] = random.choice(np.arange(1, 30000000, 3.141592)) 
        base_document['host.network.egress.packets'] = random.choice(np.arange(1, 30000)) 
        base_document['host.network.ingress.bytes'] = random.choice(np.arange(1, 30000000, 2.718))
        base_document['host.network.egress.packets'] = random.choice(np.arange(1, 30000)) 
        base_document['host.network.ingress.packets'] = random.choice(np.arange(1, 30000)) 
        base_document['host.os.family'] = random.choice(['redhat', 'debian', 'freebsd', 'windows', 'macos', 'ubuntu', 'fedora','solaris']) 
        base_document['host.os.full'] = base_document['host.os.family']
        base_document['host.os.kernel'] = ''
        base_document['host.os.name'] = base_document['host.os.family'].capitalize()
        base_document['host.os.platform'] = ''
        base_document['host.os.type'] = '' 
        base_document['host.os.version'] = ''
        base_document['host.pid_ns_ino'] = '' 
        base_document['host.type'] = random.choice(['kubernetes_node', 'kubernetes_server', 'openshift_server', 'openshift_node', 'container', 'virtual_machine', 'cloud_host', 'physical_device'])
        base_document['host.uptime'] = ''
        return base_document

    def cve_processor(self, base_document):
        cve_ids = self.generate_cve_list(10000)
        base_document['cve_list'] = list(set([random.choice(cve_ids) for x in range(random.choice(range(10)))]))
        return base_document

    def generate_cve_list(self, size):
        recent_cve_documents_pipeline = {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "lastModifiedDate": {
                          "gte": "2022-04-01T00:00:00.000Z"
                        }
                      }
                    }
                  ]
                }
            }
        any_cve_documents_pipeline = {
            "bool": {
              "filter": [
                {
                  "range": {
                    "lastModifiedDate": {
                      "gte": "2021-01-01T00:00:00.000Z"
                    }
                  }
                }
              ]
            }
        }
        recent_cve_documents = self.es.search(index='nvd_recent', query=recent_cve_documents_pipeline, source=False, size=1000)['hits']['hits']
        any_cve_documents = self.es.search(index='nvd', query=any_cve_documents_pipeline, source=False, size=1000)['hits']['hits']
        cve_list = [x['_id'] for x in recent_cve_documents] + [x['_id'] for x in any_cve_documents]
        return cve_list
