from  random import randint
import subprocess
import json
from random import randrange
from datetime import timedelta, datetime
from requests import delete as rdelete, put as rput

def DocumentFactory(schema):        
    def pad(i, l):
        return (('0'*l)+str(i))[-l:]
            
    word_file = "/usr/share/dict/words"
    words = open(word_file).read().splitlines()
    num_words = len(words)

    min_y = 1950
    min_m = 1
    min_d = 1
    max_y = 2014
    max_m = 7
    max_d = 1

    categories = {}
    dates = {}
    texts = {}
    numbers = {}
    for key, value in schema.iteritems():
        if value == 'string':
            categories[key] = [words[randint(0, num_words-1)] for i in range(randint(2, 10))] 
        elif value == 'text':
            texts[key] = [words[randint(0, num_words-1)] for i in range(100)] 
        elif value == 'date':
            l_min_y = randint(min_y, max_y)
            l_min_m = randint(min_m, max_m)
            l_min_d = randint(min_d, max_d)
            l_max_y = pad(randint(l_min_y, max_y),4)
            l_max_m = pad(randint(l_min_m, max_m),2)
            l_max_d = pad(randint(l_min_d, max_d),2)
            l_min_y = pad(l_min_y,4)
            l_min_m = pad(l_min_m,2)
            l_min_d = pad(l_min_d,2)
            d1 = datetime.strptime('/'.join([l_min_y, l_min_m, l_min_d]), '%Y/%m/%d')
            d2 = datetime.strptime('/'.join([l_max_y, l_max_m, l_max_d]), '%Y/%m/%d')
            dates[key] = {'min': d1, 'max': d2}
        elif value == 'number':  
            _min = randint(-100000, 100000)
            _max = randint(_min, 100001)
            numbers[key] = {'min': _min, 'max': _max}            

    def random_date(start, end):
        """
        This function will return a random datetime between two datetime 
        objects.
        """
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def Document():
        doc = {}
        for key, value in schema.iteritems():
            if value == 'string':
                cats = categories[key]
                num_choices = len(cats)
                doc[key] = cats[randint(0, num_choices-1)]
            elif value == 'text':
                text = texts[key]
                doc[key] = ' '.join([text[randint(0, 99)] for i in range(randint(10, 50))])
            elif value == 'date':
                date = dates[key]
                t = random_date(date['min'], date['max']) 
                doc[key] = t.strftime('%Y-%m-%d')
            elif value == 'number':
                num = numbers[key]
                doc[key] = randint(num['min'], num['max'])
        return doc
    return Document

def seed(size, schema):
    rdelete("http://localhost:9200/bokeshif")

    schema = json.loads(open('scripts/schemas/{0}.json'.format(schema)).read())    
    Document = DocumentFactory(schema)
    for i in range(size):
        doc = json.dumps(Document())
        cmd = "curl -XPOST 'http://localhost:9200/bokeshif/seed/' -d '{0}'".format(doc) 
        subprocess.call(cmd, shell=True,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    props = {}
    for key in [k for k in schema.keys() if schema[k] == 'string']:
        props[key] = {
            "type": "string",
            "index": "not_anaylzed"
        }
    print json.dumps(props)
    rput('http://localhost:9200/bokeshif/seed/_mapping', 
         data={
             "properties": props
         })
    
