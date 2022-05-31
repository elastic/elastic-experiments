import os, requests, time, xmltodict, datetime, ndjson, random
from elasticsearch import Elasticsearch, AuthorizationException, NotFoundError
from elasticsearch.helpers import bulk, parallel_bulk, streaming_bulk
from faker import Faker
import pytest
from elasticsearch import ApiError

container_host = 'localhost'
elastic_user = 'elastic'
elastic_password = 'elastic_password'
es1_url = f'https://{container_host}:9200'
es2_url = f'https://{container_host}:9201'

def get_es_instance():
    es = Elasticsearch(es1_url, 
                       basic_auth=(elastic_user, elastic_password), 
                       verify_certs=False)
    return es

def get_es2_instance():
    es2 = Elasticsearch(es2_url, 
                       basic_auth=(elastic_user,elastic_password), 
                       verify_certs=False)
    return es2

def get_test_es_instance():
    test_es = Elasticsearch(es1_url, 
                            basic_auth=('acme_admin','acme_admin'), 
                            verify_certs=False)
    return test_es

def tear_down_tests(es, node_up_settings):
    try:
        es.indices.delete(index='acme_hr')
        es.cluster.put_settings(body=node_up_settings)
        es.indices.delete(index='acme_hr_backup')
        es.snapshot.delete(snapshot='cluster_state', repository='acme_backups')
        indices = [x['index'] for x in es.cat.indices(format='json')]
        for index in indices:
            es.indices.delete(index=index)
        assert es.security.delete_role(name='acme_admin')['found'] == True
        assert es.security.delete_user(username='acme_admin')['found'] == True
        assert es.snapshot.delete_repository(name='acme_backups')['acknowledged'] == True
        return True
    except Exception as e:
        return e

def tear_down_cross(es, es2):
    try:
        es2.indices.delete(index='ccs_test')
        # tear down ccs
        es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : [f"localhost:9301" ]}}}}, 'transient': {}})
        # tear down ccr
        ## reset remote cluster configurations
        es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : []}}}}})
        es2.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : []}}}}})
        ## delete indexes
        es2.indices.delete(index='acme_hr_followed')
        ## delete ccr_roles
        es.security.delete_role(name='ccr_role')['found']
        es2.security.delete_role(name='remote_ccr_role')['found']
        ## delete remote ccr user
        es2.security.delete_user(username='ccr_system')['found']
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
def es():
    return get_es_instance()

@pytest.fixture
def es2():
    return get_es2_instance()

@pytest.fixture
def test_es():
    return get_test_es_instance()                            
                            
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
def node_up_settings():
    return {
            "persistent": {
                "cluster.routing.allocation.disk.watermark.low": f"90%",
                "cluster.routing.allocation.disk.watermark.high": f"95%",
                "cluster.routing.allocation.disk.watermark.flood_stage": f"97%"
            }
        }

@pytest.fixture
def test_event():
    return {
        '@timestamp': '2022-05-04T16:58:01.665061',
        'first_name': 'Logan',
        'last_name': 'Morales',
        'middle_name': 'Rose',
        'telephone_number': '442-171-6239x548',
        'email': 'kendra64@example.com',
        'employee-id': 'f4ef83f7-1536-4ba7-9e64-d61ccb5e8f49',
        'bio': 'Truth society reason standard chance history cup. Prove summer church add push culture more. Bank school system friend bed.',
        'ip_address': '206.143.154.103',
        'hired_date': '2016-05-13',
        'paid_amount': 952417,
        'days_in_service': 2182,
        'skills': [
            {'skill_name': 'mechanic', 'skill_level': 87},
            {'skill_name': 'photographic memory', 'skill_level': 56}
        ]
    }

@pytest.fixture
def repository_container_name():
    return "ccs_ccr_test_es01_1"

@pytest.fixture
def repository():
    return {
        'name': "acme_backups", 
        'type': "fs", 
        'settings': {'location': "/usr/share/elasticsearch/backups","compress":False}
    }

@pytest.fixture
def acme_role():
    return {
        "run_as": [],  
        "cluster": ["monitor"],
        "indices": [
            {
              "allow_restricted_indices": False,  
              "names": [ "acme_hr" ],
              "privileges": ["read","view_index_metadata"],
              "field_security" : { 
                "grant" : [ "first_name", "last_name", "age", "email", "employee-id", "telephone_number" ]
              },
              "query": '{"term":{"skills.skill_name.keyword":{"value":"historian"}}}' 
            }
        ],
        "metadata" : { 
            "version" : 1
        }
    }

@pytest.fixture
def test_document():
    return {
        'first_name': 'Logan',
        'last_name': 'Ecks',
        'telephone_number': '7033986004',
        'email':'jessembacon@icloud.com',
        'employee-id': '1234-5678-91011'
    }

@pytest.fixture
def ccr_role():
    return {
      "cluster": [
        "read_ccr"
      ],
      "indices": [
        {
          "names": [
            "leader-index-name"
          ],
          "privileges": [
            "monitor",
            "read"
          ]
        }
      ]
    }

@pytest.fixture
def remote_ccr_role():
    return {
      "cluster": [
        "manage_ccr"
      ],
      "indices": [
        {
          "names": [
            "follower-index-name"
          ],
          "privileges": [
            "monitor",
            "read",
            "write",
            "manage_follow_index"
          ]
        }
      ]
    }

class TestClusterManagement:
    
    def test_es_is_alive(self, es, es2):
        assert len(es.cat.nodes(format='json')) >= 1
        assert len(es2.cat.nodes(format='json')) >= 1

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
        time.sleep(1)
        assert int(es.cat.count(index='acme_hr', format='json')[0]['count']) >= 1000
    
    def test_and_diagnose_repair_shard_issue(self, es, node_up_settings, test_event):
        cluster_data = es.cluster.allocation_explain(index='acme_hr', shard=0, primary=False, include_disk_info=True)['cluster_info']['nodes']
        nodes = list(cluster_data.keys())
        primary_shard_node = [ x['node'] for x in es.cat.shards(index='acme_hr', format='json') if x['prirep'] == 'p'][0]
        primary_shard_node_id = [x for x in cluster_data if cluster_data[x]['node_name'] == primary_shard_node][0]
        primary_shard_node_stats = cluster_data[primary_shard_node_id]
        mark_twain = (primary_shard_node_stats['least_available']['free_disk_percent'] + \
        primary_shard_node_stats['most_available']['free_disk_percent'])/2
        node_down_settings = {
            "persistent": {
                "cluster.routing.allocation.disk.watermark.low": f"{mark_twain}%",
                "cluster.routing.allocation.disk.watermark.high": f"{mark_twain}%",
                "cluster.routing.allocation.disk.watermark.flood_stage": f"{mark_twain}%"
            }
        }
        es.cluster.put_settings(body=node_down_settings)
        try: 
            es.index(document=test_event, index='acme_hr')
        except ApiError as e:
            assert e.error == 'cluster_block_exception'
        es.cluster.put_settings(body=node_up_settings)
        assert es.index(document=test_event, index='acme_hr')['result'] == 'created'
    
    def test_backup_repository(self, es, repository):
        assert es.snapshot.create_repository(name=repository['name'], 
                                      type=repository['type'],
                                      settings=repository['settings'], verify=False
                                     )['acknowledged'] == True
        assert 'acme_backups' in es.snapshot.get_repository(name='acme_backups')
    
    def test_backup_cluster_settings(self, es, repository_container_name):
        os.system(f'docker exec -u 0 {repository_container_name} chown elasticsearch:elasticsearch /usr/share/elasticsearch/backups')
        es.snapshot.create(indices=['acme_hr'], 
                           include_global_state=True, 
                           repository='acme_backups', 
                           wait_for_completion=True,
                          snapshot='cluster_state')
        assert es.snapshot.get(repository='acme_backups', snapshot='cluster_state').raw['snapshots'][0]['snapshot'] =='cluster_state' 
    
    def test_restore_cluster_settings(self, es):
        unique = str(datetime.datetime.timestamp(datetime.datetime.now())).split('.')[0]
        assert es.snapshot.restore(snapshot='cluster_state', 
                            repository='acme_backups', 
                            rename_pattern='(.+)',
                            rename_replacement=f'restored_index_{unique}_$1',
                            include_aliases=False
                           )['accepted'] == True
        assert f'restored_index_{unique}_acme_hr' in es.indices.get(index=f'restored_index_{unique}_acme_hr')
    
    def test_create_snapshot(self, es):
        if es.license.get_trial_status()['eligible_to_start_trial'] == True:
            assert es.license.post_start_trial(acknowledge=True)['trial_was_started'] == True
        assert es.searchable_snapshots.mount(snapshot='cluster_state', 
                                  repository='acme_backups',
                                  index='acme_hr',
                                  renamed_index='acme_hr_backup',
                                  index_settings={'index.number_of_replicas':0},
                                  ignore_index_settings=['index.refresh_interval']
                                 )['accepted'] == True
        time.sleep(1)
        ## Search Snapshot
        assert len(es.search(index='acme_hr_backup', size=1)['hits']['hits']) == 1
    
    def test_create_rbac_policy(self, es):
        # Create RBAC test
        users = list(es.security.get_user().keys())
        assert 'acme_admin' not in users
        ## Create user
        es.security.put_user(username='acme_admin', 
                             enabled=True, 
                             email='jessembacon@icloud.com',
                             full_name="Jesse Marlon Bacon",
                             metadata={'job_function':'test_developer'},
                             password='acme_admin',
                             roles=[]
                            )
        ### get users
        users = list(es.security.get_user().keys())
        assert 'acme_admin' in users
        
    def test_rbac_policy_verification(self, es, test_es, acme_role, test_document):
        ### verify privileges
        caught = False
        try:
            test_es.cluster.state()
        except AuthorizationException as e:
            caught = True
        if caught:
            assert True
        else:
            assert False
        ### get user privileges
        privileges = test_es.security.get_user_privileges()
        for privilge_set in privileges:
            assert len(privileges[privilge_set]) == 0
        ## Create a role
        ### create or update roles
        try:
            es.security.get_role(name='acme_admin')
            es.security.delete_role(name='acme_admin')
        except NotFoundError as e:
            pass
        except AuthorizationException as e:
            raise e
        assert es.security.put_role(name='acme_admin',
                             run_as=acme_role['run_as'],
                             cluster=acme_role['cluster'],
                             indices=acme_role['indices'],
                             metadata=acme_role['metadata']
                            )['role']['created'] == True
        ## Set RBAC for user
        assert 'created' in  es.security.put_user(username='acme_admin', 
                             enabled=True, 
                             email='jessembacon@icloud.com',
                             full_name="Jesse Marlon Bacon",
                             metadata={'job_function':'test_developer'},
                             password='acme_admin',
                             roles=['acme_admin']
                            ) 
        assert 'acme_admin' in es.security.get_user(username='acme_admin')['acme_admin']['roles']
        ## verify RBAC for user
        ### attempt authorized action as user
        assert 'cluster_name' in test_es.cluster.state()
        assert list(test_es.search(index='acme_hr', size=1)['hits']['hits'][0]['_source'].keys()) ==['employee-id', 'telephone_number', 'last_name', 'first_name', 'email']
        ### attempt unauthorized action as user
        caught = False
        try:
            test_es.index(index='acme_hr', document=test_document)
        except AuthorizationException as e:
            caught = True
        if caught:
            assert True
        else:
            assert False
        ## disable user
        es.security.disable_user(username='acme_admin')

    @pytest.mark.cross
    def test_license_status(self, es, es2):
        ## Enable trial license on both clusters
        for cluster in [es, es2]:
            if cluster.license.get_trial_status()['eligible_to_start_trial'] == True:
                assert cluster.license.post_start_trial(acknowledge=True)['trial_was_started'] == True
            else:
                assert True
    
    @pytest.mark.cross
    def test_load_data_conditionally(self, es, data_set):
        if not es.indices.exists(index='acme_hr'):
            new_docs = []
            for doc in data_set:
                new = {}
                new['_source'] = doc
                new['_id'] = doc['employee-id']
                new['_index'] = 'acme_hr'
                new_docs.append(new)
            bulk(es, new_docs)
            time.sleep(1)
            assert int(es.cat.count(index='acme_hr', format='json')[0]['count']) >= 1000
        else:
            assert True
    
    @pytest.mark.cross
    def test_cross_cluster_replication_setup(self, es, es2, ccr_role, remote_ccr_role):
        node_data = es.nodes.info()['nodes']
        nodes = list(node_data.keys())
        master_nodes = [x for x in nodes if 'master' in node_data[x]['roles']]
        remote_cluster_client_nodes = [x for x in nodes if 'remote_cluster_client' in node_data[x]['roles']]
        if not remote_cluster_client_nodes == master_nodes:
            raise "Create or update nodes is not supprted for local installations.  https://www.elastic.co/guide/en/elasticsearch/reference/current/update-desired-nodes.html"
            assert False
        ## add cluster b as a remote cluster of cluster a
        assert len(es.cat.nodes(format='json')) == 3
        assert len(es2.cat.nodes(format='json')) == 3
        assert es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : [f"{container_host}:9301" ]}}}}, 'transient': {}})['acknowledged'] == True
        # Add a cross cluster user with remote_replication role on the data leader (cluster_a)
        try:
            es.security.get_role(name='ccr_role')
            es.security.delete_role(name='ccr_role')
        except NotFoundError as e:
            pass
        except AuthorizationException as e:
            raise e
        assert es.security.put_role(name='ccr_role',
                             cluster=ccr_role['cluster'],
                             indices=ccr_role['indices']
                            )['role']['created'] == True
        # add a ccr role to cluster b (requires write privileges)
        try:
            es2.security.get_role(name='remote_ccr_role')
            es2.security.delete_role(name='remote_ccr_role')
        except NotFoundError as e:
            pass
        except AuthorizationException as e:
            raise e
        assert es2.security.put_role(name='remote_ccr_role',
                             cluster=remote_ccr_role['cluster'],
                             indices=remote_ccr_role['indices']
                            )['role']['created'] == True
        # Add a cross cluster user with remote_replication role on the data leader (cluster_a)
        ## Set RBAC for user
        assert 'created' in  es2.security.put_user(username='ccr_system', 
                             enabled=True, 
                             email='jessembacon@icloud.com',
                             full_name="Jesse Marlon Bacon",
                             metadata={'job_function':'test_developer'},
                             password='acme_admin',
                             roles=['remote_ccr_role']
                            )
        assert 'remote_ccr_role' in es2.security.get_user(username='ccr_system')['ccr_system']['roles']
        # Create a follower index
        assert es2.ccr.follow(index='acme_hr_followed', leader_index='acme_hr', remote_cluster='cluster_a')['follow_index_created'] == True
    
    @pytest.mark.cross
    def test_cross_cluster_replication_data_verification(self, es2):
        # Cross cluster replication verification
        ## inspect replication progress
        ### uni-directional query cluster b
        time.sleep(1)
        assert es2.ccr.stats()['follow_stats']['indices'][0]['index'] == 'acme_hr_followed'
        assert len(es2.search(index='acme_hr_followed', size=1)['hits']['hits']) == 1
    
    @pytest.mark.cross
    def test_cross_cluster_deploy(self, es, es2):
        assert len(es.cat.nodes(format='json')) == 3
        assert len(es2.cat.nodes(format='json')) == 3
        assert es.cluster.put_settings(body={'persistent':  {"cluster" : {"remote" : {"cluster_b" : {"seeds" : [f"{container_host}:9301" ]}}}}, 'transient': {}})['acknowledged'] == True
    
    @pytest.mark.cross
    def test_cross_cluser_data_load(self, es2):
        assert es2.index(index='ccs_test', document={'test':'success'})['result'] == 'created'
        time.sleep(3)
    
    @pytest.mark.cross
    def test_cross_cluster_search(self, es):
        assert es.search(index='cluster_b:ccs_test')['hits']['hits'][0]['_source']['test'] == 'success'
    
    @pytest.mark.cross
    def test_tear_down_cross(self, es, es2):
        #assert False
        assert tear_down_cross(es, es2) == True
    
    def test_tear_down(self, es, node_up_settings):
        #assert False
        assert tear_down_tests(es, node_up_settings) == True
