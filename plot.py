from time import sleep
from flask import render_template
import threading
import signal
import sys

from flask import Flask
app = Flask(__name__)
app.debug = True

from bokeshif import Bokeshif

class Killable(threading.Thread):    
    def __init__(self, **kwargs):
        super(Killable, self).__init__(**kwargs)
        self._stop = threading.Event()
        
    def stop(self):
        self._stop.set()
        
threads = []
def cleanup_threads(*args):    
    i = 0
    for thread in threads:        
        thread.stop()
        thread.join()
        i = i+1
    return 'Cleaned up {0} threads'.format(i)

def serve_app(bk):
    while True:
        bk.reload()
        sleep(0.2)

signal.signal(signal.SIGTERM, cleanup_threads)

@app.route('/')
def index():
    bokeshif = Bokeshif()
    thread = Killable(target=serve_app, args=[bokeshif])
    thread.start()
    threads.append(thread)
    return render_template('plot.html', autoload_script=bokeshif.embed_tag)

if __name__ == '__main__':
    app.run()
