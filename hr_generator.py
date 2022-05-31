import os, requests, zipfile, json, random, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk, streaming_bulk
from faker import Faker
import yaml
from yaml import Loader
from copy import deepcopy
import numpy as np


class HRGenerator:
    
    def __init__(self):
        self.container_host = 'localhost'
        self.elastic_user = 'elastic'
        self.elastic_password = 'elastic_playground'
        self.es1_url = f'https://{self.container_host}:9200'
        self.es = Elasticsearch(self.es1_url, 
                       basic_auth=(self.elastic_user, self.elastic_password), 
                       verify_certs=False)
        self.faker = Faker()
    
    def hr_data_generator(self, document_count):
        faker = self.faker
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
            document['skills'] = self.skills_list(faker)
            documents.append(document)
        return documents
    
    def skills_list(self, faker):
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
