from datetime import datetime
import colorsys
from copy import deepcopy
from collections import OrderedDict

from bokeh.document import Document
from bokeh.session import Session
from bokeh.widgetobjects import Layout, VBox, HBox, PreText, MultiSelect, DateRangeSlider, TextInput
from bokeh.objects import  DataRange1d, ColumnDataSource, Plot, LinearAxis, Glyph, Grid, DatetimeAxis, HoverTool, DatetimeTickFormatter, PlotObject
from bokeh.properties import Instance, List, String, Dict, String, Any
from bokeh.glyphs import Quad, Circle
from bokeh.embed import autoload_server
from bokeh.plotting import figure, curplot, quad, curdoc

from datasource import ElasticDS as EDS

class BokeshifApp(PlotObject):
    session = Session()
    timeline_src = Instance(ColumnDataSource)
    layout = Instance(Layout)
    results = List(String)
    query = Dict(String, Any)
    
    def __init__(self, session=None, es_index='bokeshif'):
        super(BokeshifApp, self).__init__()
        eds = EDS()
        data = eds.get_data_frame()
        self.timeline_src = ColumnDataSource(data=data)

        self.colors = self._make_palette(len(eds.date_columns))
        xdr, ydr = self._make_ranges(self.timeline_src, eds.date_columns)
        timelines, sliders = self._make_timelines(self.timeline_src, eds.date_columns, xdr, ydr)

    @property
    def name(self):
        return curdoc().docid

    def update(self, **kwargs):
        super(BokeshifApp, self).update(**kwargs)

    def _make_ranges(self, src, cols):
        xdr = DataRange1d(sources=[src.columns('date')])
        ydr = DataRange1d(sources=[src.columns(c) for c in cols])
        return xdr, ydr


    def _make_timelines(self, src, cols, xdr, ydr):
        def timeline(key, color):
            figure(x_range=[xdr.start, xdr.end],
                   x_axis_type="datetime",  
                   y_range=[ydr.start, ydr.end],
                   title=None,
                   tools=['hover'],  
                   x_grid_line_color='black',
                   y_grid_line_color='black',
                   plot_width=675,
                   plot_height=100,
                   min_border=5,
                   min_border_left=50,
                   min_border_bottom=25,
                   min_border_right=1)       
            quad(left='left',
                 right='right', 
                 bottom=0,
                 top=key, 
                 source=src, 
                 color=color, 
                 fill_alpha=0.15)
            quad(left='left',
                 right='right',
                 bottom=0,
                 top='max',
                 source=src, 
                 color=color,
                 fill_alpha='{0}_density'.format(key))             
            return curplot()
            
        def slider(key):
            def on_change(obj, attr, old, new):
                if isinstance(new[0], float):
                    return
                else:
                    dates = [t.split('T')[0] for t in new]
                    q = {
                        'from': dates[0],
                        'to': dates[1]
                    }
                    self.query[obj._owner] = q        
                    #self._update_data()
            drs = DateRangeSlider.create(bounds=[xdr.start, xdr.end],
                                         value=[xdr.start, xdr.end],
                                         range=(dict(days=1), None),
                                         title=key,
                                         name=key)
            drs._owner = key
            drs.on_change('value', on_change)

        lines = []
        sliders = []
        for i in range(len(cols)):
            color = self.colors[i]            
            tl = timeline(cols[i], color)
            lines.append(tl)
            sl = slider(cols[i])
            sliders.append(sl)
        return lines, sliders
            
    def _make_palette(self, N=8):
        # from jhrf-- http://stackoverflow.com/questions/876853/generating-color-ranges-in-python
        HSV_tuples = [(x*1.0/N, 1.0, 0.5) for x in xrange(N)]
        hex_out = []
        for rgb in HSV_tuples:
            rgb = map(lambda x: int(x*255),colorsys.hsv_to_rgb(*rgb))
            hex_out.append("#"+"".join(map(lambda x: chr(x).encode('hex'),rgb)))
        return hex_out

