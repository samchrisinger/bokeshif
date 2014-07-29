import uuid
import time
from datetime import datetime
_fts = datetime.fromtimestamp
_spt = datetime.strptime
from webbrowser import open_new_tab

from requests.exceptions import ConnectionError

from bokeh.document import Document
from bokeh.session import Session
from bokeh.widgetobjects import *
from bokeh.objects import  DataRange1d, ColumnDataSource, Plot, LinearAxis, Glyph, Grid, DatetimeAxis
from bokeh.glyphs import Quad

from datasource import ElasticDS as EDS

document = Document()
session = Session(load_from_config=False)
docname = str(uuid.uuid4())
session.use_doc(docname)
session.load_document(document)

ds = EDS(index='bokeshif')
data = ds.get_data()['hist']
cols = ds.cols
source = ColumnDataSource(
    data=data,
    column_names=ds.cols+['left','right','bottom']
)
query = {}
counter = PreText(text='')

def update_data():
    data = ds.get_data(query)
    schema = ds.get_schema()
    counter.text = 'Showing {0} out of {1} documents'.format(data['hits'], schema['__global']['size'])
    source.data = data['hist']
    session.store_document(document)    

def _row_controls(col):
    def slide_change(obj, attr, old, new):
        if isinstance(new[0], float):
            return
        else:
            dates = [t.split('T')[0] for t in new]
            q = {
                'from': dates[0],
                'to': dates[1]
            }
            query[obj._owner] = q        
            update_data()
    
    slider = DateRangeSlider(bounds=[_fts(d) for d in ds.domain],
                                     value=[_fts(d) for d in ds.domain],
                                     range=(dict(days=1), None))                                 
    slider.on_change('value', slide_change)
    slider._owner = col
    return slider

def _row(i):
    colors = ['red', 'blue', 'green', 'yellow', 'orange', 'purple']
    color = colors[i]

    xdr = DataRange1d(sources=[source.columns('date')])
    ydr = DataRange1d(sources=[source.columns(c) for c in cols])

    plot = Plot(data_sources=[source], 
                x_range=xdr, 
                y_range=ydr, 
                title=None,
                plot_width=700,
                plot_height=100,
                min_border=0)

    quad = Quad(left='left',
                right='right',
                bottom='bottom',
                top=cols[i],
                line_color=(color),
                line_alpha=0.5)
    
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
                       dimension=1,
                       axis_label='count')

    ygrid = Grid(plot=plot,
                 dimension=1,
                 axis=yaxis)
    return VBox(children=[plot, _row_controls(cols[i])])

def _controls():
    def select_change(obj, attr, old, new):
        q = new
        query[obj._owner] = q
        update_data()
    
    schema = ds.get_schema()
    subset = {k:v for k,v in schema.iteritems() if not k == '__global' and v['type'] == 'string'}

    selects = []
    for k,v in subset.iteritems():
        ms = MultiSelect.create(
            name=k,
            options=v['cats']        
        )
        ms._owner = k
        ms.on_change('value', select_change)
        selects.append(ms)
    return VBox(children=selects)

def _info():
    schema = ds.get_schema()
    counter.text = 'Showing {0} out of {0} documents'.format(schema['__global']['size'])
    return counter


def _plot():
    rows = []
    for i in range(len(cols)):
        rows.append(_row(i))
    
    return VBox(children=([_info()]+rows))

def layout():
    layout = HBox(children=[_controls(),_plot()])
    return layout

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
