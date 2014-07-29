from pyelasticsearch import ElasticSearch
from requests import get as rget
from werkzeug.contrib.cache import SimpleCache
from datetime import datetime
import pandas as pd

class ElasticDS():
    def __init__(self, index='test'):
        self.cache = SimpleCache()
        self.es = ElasticSearch('http://localhost:9200')
        self.index = index

    def get_schema(self):
        if self.cache.get('schema'):
            return self.cache.get('schema')        
        global es
        schema = {}
        res = rget('http://localhost:9200/test/_mapping').json()
        schema = res['test']['mappings']['sample']['properties']
        subset = {k:v for k,v in schema.iteritems() if v['type'] == 'date'}
        datekeys = subset.keys()
        maxes = []
        mins = []
        aggs = {}
        for key in datekeys:
            aggs['max_'+key] = {
                "max": {
                    "field": key
                }
            }
            aggs['min_'+key] = {
                "min": {
                    "field": key
                }
            }
        subset = {k:v for k,v in schema.iteritems() if v['type'] == 'string'}
        stringkeys = subset.keys()
        for key in stringkeys:
            aggs[key+'_cats'] = {
                "terms": {
                    "field": key
                }
            }
        query = {
            "aggs": aggs,
            "size": 0
        }
        res = self.es.search(query, index='test')
        aggs = res['aggregations']
        for key in datekeys:
            maxes.append(aggs['max_'+key]['value'])
            mins.append(aggs['min_'+key]['value'])            
        schema['__global'] = {
            'times': {
                'max': max(maxes)/1000,
                'min': min(mins)/1000
            }
        }    
        for key in stringkeys:
            schema[key]['cats'] = [b['key'] for b in aggs[key+'_cats']['buckets']]
        res = rget('http://localhost:9200/{0}/_count'.format(self.index))
        schema['__global']['size'] = res.json().get('count') or 0

        self.cache.set('schema', schema)
        return schema

    def get_data(self, args={}):
        schema = self.get_schema()
        gtimes = schema['__global']['times']
        queries = []
        aggs = {}
        for key, value in args.iteritems():            
            if isinstance(value, dict):
                vals = value.keys()                
                if 'from' in value.keys() or 'to' in value.keys():
                    gmin = datetime.fromtimestamp(gtimes['min'])
                    gmax = datetime.fromtimestamp(gtimes['max'])
                    bounds = {
                        'from': gmin.strftime('%Y-%m-%d'),
                        'to': gmax.strftime('%Y-%m-%d')
                    }
                    for v in vals:
                        bounds[v] = value[v]
                    range = {}
                    range[key] = bounds
                    queries.append({"range": range})
                    window = (datetime.strptime(bounds['to'], '%Y-%m-%d')-datetime.strptime(bounds['from'], '%Y-%m-%d')).days
                    gwindow = (gmax-gmin).days
                    rat = (window/gwindow)*gwindow
                    interval = max(rat/1000,1)
                    aggs[key+'_hist'] = {
                        "date_histogram": {
                            "field": key,
                            "interval": "{0}d".format(interval)
                        }
                    }
            else:
                vals = value
                ors = []
                for val in vals:
                    match = {}
                    match[key] = val
                    filter = {
                        "fquery": {
                            "query": {
                                "match": match
                            },
                            "_cache": True
                        }
                    }
                    ors.append(filter)
                queries.append({"or": ors})
        diff = set([k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'date']) - set(args.keys())
        for key in diff:
            gmin = datetime.fromtimestamp(gtimes['min'])
            gmax = datetime.fromtimestamp(gtimes['max'])
            gwindow = (gmax-gmin).days
            interval = max(gwindow/1000,1)
            aggs[key+'_hist'] = {
                "date_histogram": {
                    "field": key,
                    "interval": "{0}d".format(interval)
                }
            }        
        qs = {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": ({
                        "and": queries
                    }) if len(queries) > 0 else {"match_all": {}},
                }
            }
        }
        if len(aggs) > 0:
            qs["aggregations"] = aggs

        res = self.es.search(qs, index='test')
        hits = [h['_source'] for h in res['hits']['hits']]
        hit_count = res['hits']['total']
        aggs = res.get('aggregations') or []

        timekeys = [k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'date']
        hist = {}
        for agg in aggs:
            bucket = aggs[agg]['buckets']
            key = agg.split('_hist')[0]
            for bin in bucket:        
                if not hist.get(bin['key']):
                    hist[bin['key']] = {k:0 for k in timekeys+['bottom', 'left', 'right']}            
                hist[bin['key']][key] = bin['doc_count']
                hist[bin['key']]['left'] = (bin['key'])-(interval/2)
                hist[bin['key']]['right'] = (bin['key'])+(interval/2)
        cells = {k:[] for k in ['bottom', 'left', 'right']+timekeys}
        cols = cells.keys()
        cells['date'] = []
        for date in hist:
            cells['date'].append(date)
            for col in cols:
                cells[col].append(hist[date][col])            

        return {
            'hits': hit_count,
            'hist': cells,
            'data': hits,
        }

    def get_data_frame(self):
        res = self.get_data()
        hist = pd.DataFrame.from_dict(res['hist'])
        return hist
        
    @property
    def domain(self):
        schema = self.get_schema()
        gtime = schema['__global']['times']
        return [gtime['min'], gtime['max']]

    @property
    def cols(self):
        schema = self.get_schema()
        return [k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'date']
