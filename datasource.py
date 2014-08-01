from pyelasticsearch import ElasticSearch
from requests import get as rget
from datetime import datetime, timedelta
from pandas import DataFrame
from json import dumps
import time

class ElasticQuery:
    def __init__(self, schema):        
        self.max_timestamp = schema['__global']['times']['max']
        self.min_timestamp = schema['__global']['times']['min']
        self.max_time = datetime.fromtimestamp(self.max_timestamp)
        self.min_time = datetime.fromtimestamp(self.min_timestamp)
        self.datekeys =  [k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'date']
        gmin = self.min_time
        gmax = self.max_time
        gwindow = (gmax-gmin).days
        self.interval = max(gwindow/1000,1)
        
    def _ors(self, ors):
        return {"or": ors}

    def _ands(self, ands):
        return {"and": ands}

    def _rangequery(self, field, args):        
        bounds = {
            'from': (self.min_time-timedelta(days=1)).strftime('%Y-%m-%d'),
            'to': (self.max_time+timedelta(days=1)).strftime('%Y-%m-%d')
        }
        bounds.update(args)
        timerange = {}
        timerange[field] = bounds
        return timerange        
        
    def _rangeagg(self, field, interval):
        return {
            "date_histogram": {
                "field": field,
                "interval": "{0}d".format(interval)
            }
        }
        
    def _matchquery(self, field, terms):
        def _filter(field, value):
            match = {}
            match[field] = value
            return {
                "fquery": {
                    "query": {
                        "match": match
                    },
                    "_cache": True
                }
            }
        if len(terms) == 1:            
            return _filter(field, terms[0])
        else: 
            ors = []
            for value in terms:
                ors.append(_filter(field, value))
            return {"or": ors}
    
    def _searchquery(self, field, search):
        s = {}
        s[field] = [search]
        return {
            "prefix": s,
        }

    def _build_qs(self, args):
        queries = []
        aggs = {}
        interval = self.interval

        for key, value in args.iteritems():            
            if isinstance(value, dict):
                if 'from' in value.keys() or 'to' in value.keys():
                    timerange = self._rangequery(key, value)
                    queries.append({"range": timerange})
                else:
                    # TODO
                    pass
            elif isinstance(value, list):
                match = self._matchquery(key, value)
                queries.append(match)
            elif isinstance(value, str) or isinstance(value, unicode):
                search = self._searchquery(key, value)
                queries.append(search)
                
        for key in self.datekeys:
            aggs[key+'_hist'] = self._rangeagg(key, interval)

        filter = {}
        if len(queries) == 0:
            filter = {"match_all": {}}
        elif len(queries) == 1:
            filter = queries[0]
        else:
            filter = {"and": queries}
        qs = {
            "query": {
                "filtered": {
                    "query": {
                        "match_all": {}
                    },
                    "filter": filter
                }
            }
        }
        if len(aggs) > 0:
            qs["aggregations"] = aggs
        return qs


    def _parse_hist(self, aggs):
        hist = {}
        for agg in aggs:
            bucket = aggs[agg]['buckets']
            key = agg.split('_hist')[0]
            for bin in bucket:        
                if not hist.get(bin['key']):
                    hist[bin['key']] = {k:0 for k in self.datekeys+['bottom', 'left', 'right', 'max']}            
                hist[bin['key']][key] = bin['doc_count']
                hist[bin['key']]['left'] = (bin['key'])-(self.interval/2)
                hist[bin['key']]['right'] = (bin['key'])+(self.interval/2)
        cells = {k:[] for k in ['bottom', 'left', 'right']+self.datekeys}
        cols = cells.keys()
        cells['date'] = []
        for date in sorted(hist.keys()):
            cells['date'].append(date)            
            for col in cols:
                cells[col].append(hist[date][col])            
                
        gmax_height = 0
        for col in self.datekeys:
            gmax_height = max([gmax_height]+cells[col])
        
        cells['max'] = [gmax_height*1.25]*len(cells['date'])    
        for col in self.datekeys:
            max_height = float(max(cells[col])) if len(cells[col]) > 0 else 0
            density = '{0}_density'.format(col)
            cells[density] = []
            for i in range(len(cells['date'])):
                cells[density].append(((cells[col][i]/max_height)*0.1))
        return cells

    def _parse_hits(self, hits):
        data = {}
        if len(hits) > 0:
            keys = hits[0].keys()            
            data = {k:[] for k in keys+['show']}
            for doc in hits:
                doc['show'] = dumps(doc)
                for key,value in doc.iteritems():
                    data[key].append(value)
        return data

    def update(self, es, index, args={}):
        qs = self._build_qs(args)        
        print dumps(qs)+'\n'

        t = time.time()
        res = es.search(qs, index=index)
        difference = time.time()-t
        es.index('bokeh_logs', 'log', {
            'query': dumps(qs),
            'took': difference
        })
        hits = [h['_source'] for h in res['hits']['hits']]
        hit_count = res['hits']['total']
        aggs = res.get('aggregations') or []
        cells = self._parse_hist(aggs)
        data = self._parse_hits(hits)
        
        ret = {
            'hits': hit_count,
            'hist': cells,
            'data': data,
        }
        return ret

class ElasticDS:
    def __init__(self, index='bokeshif'):
        self._cache = {}
        self.es = ElasticSearch('http://localhost:9200')
        self.index = index
        schema = self.get_schema()
        self.query = ElasticQuery(schema)

    def get_schema(self):
        if self._cache.get('schema'):
            return self._cache['schema']
        global es
        schema = {}
        res = rget('http://localhost:9200/{0}/_mapping'.format(self.index)).json()
        schema = res[self.index]['mappings']['seed']['properties']
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
        print dumps(query)
        res = self.es.search(query, index=self.index)
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
            qs = {
                "query": {
                    "filtered": {
                        "query": {
                            "match_all": {}
                        },
                        "filter": {
                            "script": {
                                "script": unicode("doc['{0}'].values.size() > 1".format(key))
                            }
                        }
                    }
                }
            }                            
            res = self.es.count(qs, index=self.index)
            if (res.get('count') or 0) == 0:                            
                # categorical
                schema[key]['cats'] = [b['key'] for b in aggs[key+'_cats']['buckets']]
            else:
                # text
                schema[key]['type'] = 'text'                
                schema[key]['cats'] = [b['key'] for b in aggs[key+'_cats']['buckets']]

        res = rget('http://localhost:9200/{0}/_count'.format(self.index))
        schema['__global']['size'] = res.json().get('count') or 0

        self._cache['schema'] = schema
        return schema

    def get_data(self, args={}):
        if len(args) == 0 and self._cache.get('data'):
            return self._cache['data']        
        ret = self.query.update(self.es, self.index, args)            
        if len(args) == 0:
            self._cache['data'] = ret
        return ret

    def get_data_frame(self):
        res = self.get_data()
        hist = DataFrame.from_dict(res['hist'])
        return hist
        
    @property
    def domain(self):
        schema = self.get_schema()
        gtime = schema['__global']['times']
        return [gtime['min'], gtime['max']]

    @property
    def date_columns(self):
        schema = self.get_schema()
        return [k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'date']
    
    @property
    def string_columns(self):
        schema = self.get_schema()
        return [k for k in schema.keys() if not k == '__global' and schema[k]['type'] == 'string']

    @property
    def columns(self):
        schema = self.get_schema()
        return [k for k in schema.keys() if not k == '__global']
