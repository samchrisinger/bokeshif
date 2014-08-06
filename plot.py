from flask import render_template
from bokeh.plotting import cursession
from bokeh.resources import Resources

from flask import Flask
app = Flask(__name__)
app.debug = True

from bokeshif import BokeshifApp

def make_plot(session):
    bokeshif = BokeshifApp(session=session)
    return bokeshif

@app.route('/')
def index():
    sess = cursession()
    bokehshif = make_plot(sess)
    docname = bokehshif.name
    resources = Resources(mode='inline')
    return render_template('plot.html', 
                           docname=docname,
                           resources=resources,
                           bokeh_location='localhost:5006/')

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    app.debug = True
    app.run()
