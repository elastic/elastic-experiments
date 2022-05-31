# Assemble code into a class
import os, requests, zipfile, json, time, xmltodict, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk, streaming_bulk
import sys, logging
import numpy as np
import pandas as pd
import yaml
from yaml import Loader
from copy import deepcopy

class NVDLoader:
    
    def __init__(self, instance_type='local'):
        if instance_type == 'local':
            try:
                self.elastic_user = 'elastic'
                self.elastic_password = 'elastic_playground'
                self.elastic_url = 'https://localhost:9200'
                self.client = Elasticsearch(self.elastic_url, basic_auth=(self.elastic_user, self.elastic_password),verify_certs=False)
            except Exception as e:
                logging.warning('You must add credentials to the class init')
                logging.warning(e)
                sys.exit
        elif instance_type == 'elastic_cloud':
            try:
                self.elastic_user = os.environ['ELASTIC_USER']
                self.elastic_password = os.environ['ELASTIC_CLOUD_PASSWORD']
                self.elastic_cloud_id = os.environ['ELASTIC_CLOUD_ID']
                self.client = Elasticsearch(cloud_id=self.elastic_cloud_id,http_auth=(self.elastic_user, self.elastic_password))
            except Exception as e:
                logging.warning('You must add ELASTIC_USER, ELASTIC_CLOUD_PASSWORD and ELASTIC_CLOUD_ID to the container environment variables')
                logging.warning(e)
                sys.exit
            self.instance_type = instance_type
        self.two_hour_stream_feeds = {
            'CVE-Modified':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-modified.json.zip',
            'CVE-Recent':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-recent.json.zip'
        }
        self.historic_database = {
            'CVE-2022':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2022.json.zip',
            'CVE-2021':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2021.json.zip',
            'CVE-2020':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2020.json.zip',
            'CVE-2019':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2019.json.zip',
            'CVE-2018':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2018.json.zip',
            'CVE-2017':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2017.json.zip',
            'CVE-2016':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2016.json.zip',
            'CVE-2015':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2015.json.zip',
            'CVE-2014':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2014.json.zip',
            'CVE-2013':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2013.json.zip',
            'CVE-2012':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2012.json.zip',
            'CVE-2011':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2011.json.zip',
            'CVE-2010':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2010.json.zip',
            'CVE-2009':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2009.json.zip',
            'CVE-2008':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2008.json.zip',
            'CVE-2007':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2007.json.zip',
            'CVE-2006':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2006.json.zip',
            'CVE-2005':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2005.json.zip',
            'CVE-2004':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2004.json.zip',
            'CVE-2003':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2003.json.zip',
            'CVE-2002':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-2002.json.zip'
        }
        self.cpe_match_feed = {
            'CPE-Match':'https://nvd.nist.gov/feeds/json/cpematch/1.0/nvdcpematch-1.0.json.zip'
        }
        self.cpe_dictionary_feed = {
            'CPE-Dictionary':'https://nvd.nist.gov/feeds/xml/cpe/dictionary/official-cpe-dictionary_v2.3.xml.zip'
        }
        self.cce_database_dictionary = {
            "Apple macOS Bigsur":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-macos_bigsur.xls",
            "Apple macOS Catalina":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/CCE-macos_catalina-mscp.xls",
            "Red Hat Enterprise Linux 4":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-rhel4-5.20090506.xls",
            "Red Hat Enterprise Linux 5":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-rhel5-5.20111007.xls",
            "Red Hat Enterprise Linux 6":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/CCE-rhel6.xlsx",
            "Red Hat Enterprise Linux 7":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/CCE-rhel7.xlsx",
            "Red Hat Enterprise Linux 8":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/CCE-rhel8.xlsx",
            "Red Hat OpenShift Container 3":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/CCE-ocp3.xlsx",
            "VMware":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-vmware.xlsx",
            "Apache HTTP 1.3":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-apache-httpd1.3-5.20130214.xls",
            "Apache HTTP 2.0":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-apache-httpd2.0-5.20130214.xls",
            "Apache HTTP 2.2":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-apache-httpd2.2-5.20130214.xls",
            "Apache Tomcat 4":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-tomcat4-5.20130214.xls",
            "Apache Tomcat 5":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-tomcat5-5.20130214.xls",
            "Apache Tomcat 6":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-tomcat6-5.20130214.xls",
            "IIS 5":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-iis5-5.20130214.xls",
            "IIS 6":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-iis6-5.20130214.xls",
            "MS SQL 2000":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-ms-sql-2000-5.20130214.xls",
            "MS SQL 2005":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-ms-sql-2005-5.20130214.xls",
            "Microsoft Office 2007":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-office2k7-5.20130214.xls",
            "Microsoft Office 2010":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-office2010-5.20130214.xls",
            "Polycom HDX 3.X":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-polycom-hdx3-5.20120521.xls",
            "Windows 7":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-win7-5.20120521.xls",
            "Internet Explorer 7":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-ie7-5.20120314.xls",
            "Microsoft Exchange 2007":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-exchange2007-5.20120314.xls",
            "Microsoft Exchange 2010":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-exchange2010-5.20120314.xls",
            "Windows Server 2008":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-win2k8-5.20120314.xls",
            "Windows Server 2008 R2":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-win2k8r2-5.20120314.xls",
            "Windows Vista":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-vista-5.20120314.xls",
            "Windows XP":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-winxp-5.20120314.xls",
            "Oracle WebLogic Server 11g":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-weblogicserver11g-5.20111007.xls",
            "Internet Explorer 8":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-ie8-5.20100926.xls",
            "Sun Solaris 10":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-solaris10-5.20100428.xls",
            "Windows 2000":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-win2k-5.20100428.xls",
            "Windows Server 2003":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-win2k3-5.20100428.xls",
            "AIX 5.3":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-aix5.3-5.20090506.xls",
            "HP-UX 11.23<":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-hpux11.23-5.20090506.xls",
            "Sun Solaris 8":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-solaris8-5.20090506.xls",
            "Sun Solaris 9":"https://csrc.nist.gov/CSRC/media/Projects/national-vulnerability-database/documents/CCE/cce-solaris9-5.20090506.xls"
        }
    
    def download_files(self, dictionary, output_path=os.path.join(os.curdir, 'demo', 'data'), verbose=False):
        if not os.path.isdir(output_path):
            os.mkdir(output_path)
        for key in dictionary:
            target = dictionary[key]
            target_file = target.split('/')[-1]
            if verbose == True:
                print(f'Now Fetching {target_file}')
            with open(os.path.join(output_path, target_file), "wb") as f:
                f.write(requests.get(target).content)

    def extract_archives(self, data_path, output_path=os.path.join(os.curdir, 'demo', 'data', 'db'), verbose=False):
        if not os.path.isdir(output_path):
            os.mkdir(output_path)
        files = os.listdir(data_path)
        for file in files:
            if not os.path.isdir(file) and file.endswith('zip'):
                if verbose == True:
                    print(f'Extracting: {file} to {output_path}')
                zipfile.ZipFile(os.path.join(data_path, file)).extractall(output_path)

    def clean_download_directory(self, dictionary, output_path=os.path.join(os.curdir, 'demo', 'data'), clean_db=True, verbose=True):
        for key in dictionary:
            target_file = dictionary[key].split('/')[-1]
            if verbose == True:
                print(f'Now removing {target_file} from {output_path}')
            os.remove(os.path.join(output_path, target_file))
            if clean_db == True:
                os.remove(os.path.join(os.path.join(output_path, 'db'), target_file.rstrip('.zip')))

    def ingest_bulk_json_dataset(self, file_list, target_index, data_path=os.path.join(os.curdir, 'demo', 'data', 'db'), verbose=True, ingest_method='parallel_bulk'):
        task_queue = len(file_list)
        i = 0
        count = 0
        errors = []
        if not target_index in [x['index'] for x in self.client.cat.indices(format='json')]:
            self.client.indices.create(index=target_index)
        for file in file_list:
            data = []
            if file.endswith('son'):
                i += 1
                if verbose == True:
                    print(f'round: {i}: Now ingesting {file} from {data_path} to {target_index}')
                with open(os.path.join(data_path, file), 'r') as f: 
                    cve_data = json.loads(f.read())
                items = cve_data['CVE_Items']
                count += len(items)
                for item in items:
                    record = {}
                    record['_id'] = item['cve']['CVE_data_meta']['ID']
                    #record['_op_type'] = 'insert'
                    record['_index'] = target_index
                    record['doc_type'] = 'cve'
                    record['_source']  = item
                    data.append(record)
                if ingest_method == 'parallel_bulk':
                    for success, info in parallel_bulk(self.client, data):
                        if not success:
                            if verbose == True:
                                print('A document failed:', info)
                            errors.append(info)
                elif ingest_method == 'bulk':
                    bulk(self.client, data)
                elif ingest_method == 'streaming_bulk':
                    successes = 0
                    for ok, success in streaming_bulk(client=self.client, index=target_index, actions=data):
                        successes += ok
                elif ingest_method == 'singleton':
                    for item in items:
                        self.client.index(index=target_index, document=item, id=item['cve']['CVE_data_meta']['ID'])
                if verbose == True:
                    if i % 2 == 0:
                        print(f'The ingest process is %{round((i/task_queue) * 100, 2)} complete')
        if ingest_method in ['singleton','bulk', 'streaming_bulk']:
            return f'{count} documents sent to elasticsearch'
        elif ingest_method == 'parallel_bulk':
            return f'{count} documents sent to elasticsearch, {len(errors)} networking errors were detected during the transfer'

    def document_total_for_directory(self, file_list, data_path=os.path.join(os.curdir, 'demo', 'data', 'db'), verbose=True):
        i=0
        document_sum = 0
        cve_ids = []
        for file in file_list:
            i += 1
            if file.endswith('son'):
                if verbose == True:
                    print(f'round: {i}: Now counting CVE items in {file} from {data_path}')
                with open(os.path.join(data_path, file), 'r') as f: 
                    cve_data = json.loads(f.read())
                items = cve_data['CVE_Items']
                for item in items:
                    cve_ids.append(item['cve']['CVE_data_meta']['ID'])
                document_sum += len(items)
        return {'total_read_documents':document_sum, 'unique_cve_ids':len(set(cve_ids))}

    def test_ingest_methods(self, file_list=sorted(os.listdir(os.path.join(os.curdir, 'demo', 'data', 'db'))), data_path=os.path.join(os.curdir, 'demo', 'data', 'db'), sleep_time_between_tests=180, skip_singleton=False):
        document_data = self.document_total_for_directory(file_list=file_list, data_path=data_path, verbose=False)
        report = {}
        for technique in ['singleton', 'bulk', 'parallel_bulk', 'streaming_bulk']:
            print(f'Now testing {technique} ingest technique')
            if skip_singleton == True:
                if technique == 'singleton':
                    pass
                else:
                    try:
                        self.client.indices.delete(index='nvd')
                    except Exception as e:
                        pass
                    self.ingest_bulk_json_dataset(file_list=file_list, ingest_method=technique, target_index='nvd', verbose=False)
                    time.sleep(sleep_time_between_tests)
                    report[technique] = self.client.cat.indices(index='nvd', format='json')[0]
        return report

    def update_cve_data(self, dictionary, data_path=os.path.join('demo', 'data'), target_index='nvd', update_method='streaming_bulk'):
        self.download_files({'file':dictionary['CVE-Modified']})
        self.extract_archives(data_path=data_path)
        output = self.ingest_bulk_json_dataset(['nvdcve-1.1-modified.json'], target_index, data_path=os.path.join(data_path, 'db'), verbose=True, ingest_method=update_method)
        self.clean_download_directory(dictionary={'update_feed':'https://nvd.nist.gov/feeds/json/cve/1.1/nvdcve-1.1-modified.json.zip'}, output_path=data_path)
        return output

    def create_nvd_recent_index(self, data_path=os.path.join('demo', 'data'), target_index='nvd_recent'):
        try:
            self.client.index.delete('nvd_recent')
        except:
            pass
        self.download_files({'file':self.two_hour_stream_feeds['CVE-Recent']})
        self.extract_archives(data_path)
        self.ingest_bulk_json_dataset([self.two_hour_stream_feeds['CVE-Recent'].split('/')[-1].rstrip('.zip')], 
                                      target_index, data_path=os.path.join(data_path, 'db'), 
                                      verbose=True, ingest_method='singleton')
        self.clean_download_directory(dictionary={'update_feed':self.two_hour_stream_feeds['CVE-Recent']}, output_path=data_path)
        return True

    def create_cpe_match_index(self, target_index, data_path=os.path.join('demo', 'data'), output_path=os.path.join('demo', 'data', 'db')):
        if not target_index in [x['index'] for x in self.client.cat.indices(format='json')]:
            self.client.indices.create(index=target_index)
        self.download_files({'file':self.cpe_match_feed['CPE-Match']})
        self.extract_archives(data_path)
        errors = []
        with open(os.path.join(output_path, self.cpe_match_feed['CPE-Match'].split('/')[-1].rstrip('.zip')), 'r') as f:
            data = json.loads(f.read())
        #  only singleton is working with this data
        for document in data['matches']:
            self.client.index(document=document, index=target_index)
        # bulk
        #output = []
        #for item in data['matches']:
        #    record = {}
        #    #record['_op_type'] = 'insert'
        #    record['_index'] = target_index
        #    record['doc_type'] = 'cpe_match'
        #    record['_source']  = item
        #    output.append(record)
        #bulk(self.client, output)
        self.clean_download_directory(dictionary={'match_feed':self.cpe_match_feed['CPE-Match'].split('/')[-1]})
        return 

    def load_cpe_dictionary(self, target_file, target_index='cpe_dictionary', ingest_method='bulk'):
        errors = []
        with open(target_file, 'r') as f:
            data = xmltodict.parse(f.read())
        if ingest_method=='singleton':
            for item in data['cpe-list']['cpe-item']:
                es.index(document=item, index='cpe_dictionary', id=item['@name'])
            return self.client.cat.indices(index=target_index, format='json')[0]
        else:
            records = []
            for document in data['cpe-list']['cpe-item']:
                record = {}
                record['_source'] = document
                record['_id'] = document['@name']
                record['_index'] = target_index
                record['doc_type'] = 'cpe_record'
                records.append(record)
            if ingest_method == 'parallel_bulk':
                for success, info in parallel_bulk(self.client, records):
                    if not success:
                        if verbose == True:
                            print('A document failed:', info)
                        errors.append(info)
                return self.client.cat.indices(index=target_index, format='json')[0], errors
            elif ingest_method == 'bulk':
                bulk(self.client, records)
                return self.client.cat.indices(index=target_index, format='json')[0]
            elif ingest_method == 'streaming_bulk':
                successes = 0
                for ok, success in streaming_bulk(client=self.client, index=target_index, actions=records):
                    successes += ok
                    return self.client.cat.indices(index=target_index, format='json')[0], successes
    
    def load_cce_data(self, file_list, data_path, verbose=True):
        i =0 
        for file in file_list:
            skip_rows = 0
            toggle = False
            if file.endswith('xls') or file.endswith('xlsx'):
                while toggle == False:
                    df = pd.read_excel(os.path.join(data_path, file))
                    if df.columns[1].startswith('Last') or df.columns[1].startswith('Version'):
                        skip_rows += 1
                    df = pd.read_excel(os.path.join(data_path, file), skiprows=skip_rows)
                    if not df.columns[1].startswith('Last') and not df.columns[1].startswith('Version'):
                        toggle = True
                if verbose == True:
                    print(f'Now logging {file} to the Elasticsearch Common Configuration Enumeration definitions index')
                db =  df.T.to_dict()
                for row in db:
                    document = db[row]
                    d2 = deepcopy(document)
                    for k,v in document.items():
                        if not type(v) == str:
                            if type(v) == float:
                                if not np.isfinite(v):
                                    d2.pop(k)
                    if len(d2.keys()) < 1:
                        break
                    else:
                        keys = d2.keys()
                        if 'CCE' in keys:
                            _id = d2['CCE']
                        elif 'CCE ID' in keys:
                            _id = d2['CCE ID']
                        elif 'CCE ID v5' in keys:
                            _id = d2['CCE ID v5']
                        document['data_file'] = file
                        self.client.index(document=d2, index='cce', id=_id)
                i += 1
                if verbose == True:
                    print(f'{i} documents processed')
        return