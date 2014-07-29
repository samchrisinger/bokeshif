from  random import randint
import subprocess
import json
from random import randrange
from datetime import timedelta, datetime
from requests import delete as rdelete

def pad(i, l):
    return (('0'*l)+str(i))[-l:]

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def Date(d):
    return {
        'type': 'date',
        'value': d.strftime('%Y-%m-%d')
        }

def String(s):
    return {
        'type': 'string',
        'value': s
    }


word_file = "/usr/share/dict/words"
words = open(word_file).read().splitlines()
num_words = len(words)

categories = {}
for i in range(6):
    name = words[randint(0,num_words)]
    categories[name] = [words[randint(0, num_words)] for i in range(randint(2, 10))]    
times = {}

min_y = 1950
min_m = 1
min_d = 1
max_y = 2014
max_m = 7
max_d = 1
for i in range(4):
    name = words[randint(0,num_words)]
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
    times[name] = {'min': d1, 'max': d2}
    
def Document():
    doc = {'map': {}}
    for cat in categories:
        choice = categories[cat][randint(0, len(categories[cat])-1)]
        doc[cat] = choice
    for time in times:
        t = random_date(times[time]['min'], times[time]['max']) 
        doc[time] = t.strftime('%Y-%m-%d')
    return doc

def seed(size):
    rdelete("http://localhost:9200/bokeshif")
    for i in range(size):
        doc = json.dumps(Document())
        cmd = "curl -XPOST 'http://localhost:9200/bokeshif/seed/' -d '{0}'".format(doc) 
        subprocess.call(cmd, shell=True,  stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    
