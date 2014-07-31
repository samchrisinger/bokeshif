from time import sleep
from flask import render_template
from threading import Thread

from flask import Flask
app = Flask(__name__)
app.debug = True

from bokeshif import Bokeshif

def serve_app(bk):
    while True:
        bk.reload()
        sleep(0.2)

@app.route('/')
def index():
    bokeshif = Bokeshif()
    thread = Thread(target=serve_app, args=[bokeshif])
    thread.start()
    return render_template('plot.html', autoload_script=bokeshif.embed_tag)

if __name__ == '__main__':
    app.run()
