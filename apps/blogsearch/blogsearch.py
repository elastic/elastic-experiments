from elasticsearch import Elasticsearch
from flask import Flask, render_template, request
import ssl

elastic_host = 'localhost'
elastic_port = 9200
elastic_user = 'elastic'
elastic_password = 'elastic_playground'
elastic_url= 'http://localhost:9200'

client = Elasticsearch(
  hosts=[elastic_url],
  basic_auth=(elastic_user, elastic_password),
  verify_certs=False,
  ssl_version=ssl.TLSVersion.TLSv1_2,
  ssl_show_warn=False
)

#logging.warning("Nodes Alive")
#logging.warning(json.dumps(client.cat.nodes(format='json').raw, indent=2))

app = Flask(__name__)

# basic functionality
@app.route("/")
def landing():
    # if method == POST grab body
    ## add query, aggs, size, from, to
    size = request.args.get('size')
    if not size or size == '':
        size = 10
    data = client.search(index='blogs', size=size, source=False,
                         fields=["title", "authors.company.keyword", "tags.use_case", "publish_date"],
                         sort=[{"publish_date": {"order": "desc"}}],
                         query={"bool": {"filter": [{"exists": {"field": "tags.use_case"}}],"must": [{"match": {"locale": "en-us"}}]}})
    return render_template('search_results.html', records=data['hits']['hits'])

@app.route("/content/<post_id>")
def content_view(post_id=0):
    data = client.get(index='blogs', id=post_id)
    return render_template('content_view.html', data=data)

# extended functionality
@app.route("/search")
def title_or_content_search():
    size = request.args.get('size')
    phrase = request.args.get('phrase')
    if not size or size == '':
        size = 10
    if not phrase or phrase == '':
        phrase = 'Istio'
    query = {"multi_match": {"query": phrase,"fields": ["title", "content"]}}
    data = client.search(index='blogs', size=size, source=False,
                         fields=["title", "authors.company.keyword", "tags.use_case", "publish_date"],
                         query=query)
    return render_template('search_results.html', records=data['hits']['hits'])

@app.route("/assignments")
def assignments():
    return render_template('assignments.html')

@app.route("/performance_reading")
def performance():
    index = 'blogs'
    query = {
        "bool": {
            "must": [
                {
                    "match": {
                        "locale.keyword": "en-us"
                    }
                }
            ],
            "filter": [
                {
                    "terms": {
                        "tags.use_case.keyword": [
                            "metrics",
                            "application performance monitoring",
                            "application perf mon (apm)"
                        ]
                    }
                }
            ],
            "should": [
                {
                    "match": {
                        "content": "latency"
                    }
                },
                {
                    "match": {
                        "content": "chunk"
                    }
                },
                {
                    "match": {
                        "content": "heap"
                    }
                },
                {
                    "match": {
                        "content": "rally"
                    }
                }
            ]
        }
    }
    aggregation = {}
    size = 10000
    _from = 0
    sort = [
        {
            "_score": {
                "order": "desc"
            }
        },
        {
            "_doc": {
                "order": "asc"
            }
        },
        {
            "publish_date": {
                "order": "desc"
            }
        }
    ]
    highlight = {
        "fields": {
            "content": {}
        }
    }
    _source = False
    fields = ['title', 'authors', 'publish_date']
    data = client.search(index=index, query=query, size=size, sort=sort, aggs=aggregation, from_=_from, highlight=highlight,
              source=_source, fields=fields)
    return render_template('search_results.html', records=data['hits']['hits'])

@app.route("/watcher_reading")
def watcher():
    index = 'blogs'
    query = {
        "bool": {
          "must": [
            {
              "match": {
                "locale.keyword": "en-us"
              }
            },
            {
              "match": {
                "content": "watcher"
              }
            }
          ],
          "should": [
            {
              "match": {
                "content": "ECK"
              }
            },
            {
              "match": {
                "content": "container"
              }
            }
          ]
        }
      }
    aggregation = {}
    size = 10000
    _from = 0
    sort = [
        {
            "_score": {
                "order": "desc"
            }
        },
        {
            "_doc": {
                "order": "asc"
            }
        },
        {
            "publish_date": {
                "order": "desc"
            }
        }
    ]
    highlight = {
        "fields": {
            "content": {}
        }
    }
    _source = False
    fields = ['title', 'authors', 'publish_date']
    data = client.search(index=index, query=query, size=size, sort=sort, aggs=aggregation, from_=_from,
                         highlight=highlight,
                         source=_source, fields=fields)
    return render_template('search_results.html', records=data['hits']['hits'])

@app.route("/kubernetes_reading")
def kubernetes():
    index = 'blogs'
    query = {
        "bool": {
            "must": [
                {
                    "match": {
                        "locale.keyword": "en-us"
                    }
                }
            ],
            "filter": [
                {
                    "terms": {
                        "tags.use_case.keyword": [
                            "container monitoring",
                        ]
                    }
                }
            ],
            "should": [
                {
                    "match": {
                        "content": "ECK"
                    }
                },
                {
                    "match": {
                        "content": "Kubernetes"
                    }
                },
                {
                    "match": {
                        "content": "Open Shift"
                    }
                },
                {
                    "match": {
                        "content": "openshift"
                    }
                }
            ]
        }
    }
    aggregation = {}
    size = 10000
    _from = 0
    sort = [
        {
            "_score": {
                "order": "desc"
            }
        },
        {
            "_doc": {
                "order": "asc"
            }
        },
        {
            "publish_date": {
                "order": "desc"
            }
        }
    ]
    highlight = {
        "fields": {
            "content": {}
        }
    }
    _source = False
    fields = ['title', 'authors', 'publish_date']
    data = client.search(index=index, query=query, size=size, sort=sort, aggs=aggregation, from_=_from,
                         highlight=highlight,
                         source=_source, fields=fields)
    return render_template('search_results.html', records=data['hits']['hits'])

# def autocomplete():
# def update_blogs():
# def xhr_loop():
