from pyelasticsearch import ElasticSearch
from bashplotlib.histogram import plot_hist

def stats():
    es = ElasticSearch('http://localhost:9200')
    index = 'bokeh_logs'
    
    query = {
        "aggs": {
            "took_stats" : {
                "extended_stats" : {
                    "field" : "took"
                }
            }
        }
    }

    res = es.search(query, index=index)

    print '-------------QUERIES---------------'
    print "    ---------------"
    print "    |Summary stats|"
    print "    ---------------"
    for key, value in res['aggregations']['took_stats'].iteritems():
        print '{0}: {1}'.format(key, value)
        
    query = {
        "query": {
            "match_all": {}
        },
        "size": 10000
    }
    cells = []
    res = es.search(query, index=index)
    for item in res['hits']['hits']:
        cells.append(item['_source']['took'])
    plot_hist(cells, height=25.0, regular=True, pch='.', xlab=True, colour='red', title="Distribution of recent queries")
    
