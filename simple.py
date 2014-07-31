import uuid
import time
from datetime import datetime
fts = datetime.fromtimestamp
from webbrowser import open_new_tab
from random import randint
from collections import OrderedDict

from requests.exceptions import ConnectionError

from bokeh.document import Document
from bokeh.session import Session
from bokeh.widgetobjects import *
from bokeh.objects import  DataRange1d, ColumnDataSource, Plot, LinearAxis, Glyph, Grid, DatetimeAxis, HoverTool
from bokeh.glyphs import Quad

document = Document()
session = Session(load_from_config=False)
docname = str(uuid.uuid4())
session.use_doc(docname)
session.load_document(document)

data = {
    'l': range(0,1000),
    'r': range(1,1001),
    'b': [0]*1000,
    'y': [randint(0,1000) for i in range(1000)]
}

cols = ['l', 'r', 'b', 'y']
source = ColumnDataSource(
    data=data,
    column_names=cols
)

def update_data():
    source.data = data
    session.store_document(document)

def _plot():
    xdr = DataRange1d(sources=[source.columns(c) for c in ['l','r']])
    ydr = DataRange1d(sources=[source.columns('y')])

    plot = Plot(data_sources=[source], 
                x_range=xdr, 
                y_range=ydr, 
                title=None,
                plot_width=1200,
                plot_height=800)

    quad = Quad(left='l',
                right='r',
                bottom='b',
                top='y',
                fill_color='red')                
    quad_renderer = Glyph(data_source=source, 
                          xdata_range=xdr, 
                          ydata_range=ydr, 
                          glyph=quad)
    plot.renderers.append(quad_renderer)
    
    xaxis = DatetimeAxis(plot=plot, 
                       location='bottom',
                       dimension=0)
    xgrid = Grid(plot=plot,
                 dimension=0,
                 axis=xaxis)
    yaxis = LinearAxis(plot=plot, 
                       location='left',
                       dimension=1)
    ygrid = Grid(plot=plot,
                 dimension=1,
                 axis=yaxis)    
    hover = HoverTool(plot=plot, 
                      tooltips=OrderedDict([
                          ("l", "@l")
                      ]),
                      always_active=True)    
    plot.tools.append(hover)
    return plot

def layout():    
    return _plot()

document.add(layout())
update_data()

if __name__ == "__main__":
    link = session.object_link(document._plotcontext)
    open_new_tab(link)
    try:
        while True:
            session.load_document(document)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print()
    except ConnectionError:
        print("Connection to bokeh-server was terminated")
