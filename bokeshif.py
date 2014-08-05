import uuid
from datetime import datetime
import colorsys
from copy import deepcopy
from collections import OrderedDict

from bokeh.document import Document
from bokeh.session import Session
from bokeh.widgetobjects import VBox, HBox, PreText, MultiSelect, DateRangeSlider, TextInput
from bokeh.objects import  DataRange1d, ColumnDataSource, Plot, LinearAxis, Glyph, Grid, DatetimeAxis, HoverTool, DatetimeTickFormatter
from bokeh.glyphs import Quad, Circle
from bokeh.embed import autoload_server

from datasource import ElasticDS as EDS

class Bokeshif:
    def __init__(self, es_index='bokeshif'):
        self.document = Document()
        self.session = Session(load_from_config=False)
        self.ds = EDS(index='bokeshif')
        self.session.use_doc(str(uuid.uuid4()))
        self.session.load_document(self.document)        
        self.layout = None
        self.big = True        
        self.vis = {
            'counter': PreText(text=''),
            'layout': None,
            'timelines': {
                'container': None,
            },
            'documents': {
                'width': 300,
                'height': 150,
                'container_glyph': None,
                'title_glyph': None,
                'body_glyph': None,
                'container_id': None
            }
        }
        self.query = {}
        self.count = 0

        self.timeline_source = self._make_timeline_source()
        self.document_source = self._make_document_source()
        self.colors = self._make_palette(len(self.ds.date_columns))
        
        self._update_data()
        self._make_vis()

    @property
    def link(self):
        return self.session.object_link(self.document._plotcontext)

    @property 
    def embed_tag(self):
        return autoload_server(self.layout, self.session)

    def reload(self):
        self.session.load_document(self.document)
        
    def _make_timeline_source(self):
        data = self.ds.get_data()['hist']
        cols = self.ds.date_columns
        source = ColumnDataSource(
            data=data,
            column_names=cols+['left','right','bottom']
        )
        return source

    def _make_document_source(self):
        data = self.ds.get_data()['data']
        source = ColumnDataSource(
            data=data,
        )
        return source

    def _make_ranges(self):
        xdr = DataRange1d(sources=[self.timeline_source.columns('date')])
        ydr = DataRange1d(sources=[self.timeline_source.columns(c) for c in self.ds.date_columns])
        return xdr, ydr

    def _make_palette(self, N=8):
        # from jhrf-- http://stackoverflow.com/questions/876853/generating-color-ranges-in-python
        HSV_tuples = [(x*1.0/N, 1.0, 0.5) for x in xrange(N)]
        hex_out = []
        for rgb in HSV_tuples:
            rgb = map(lambda x: int(x*255),colorsys.hsv_to_rgb(*rgb))
            hex_out.append("#"+"".join(map(lambda x: chr(x).encode('hex'),rgb)))
        return hex_out

    def _update_data(self):       
        data = self.ds.get_data(self.query)
        self.timeline_source.data = data['hist']
        self.document_source.data = data['data']
        schema = self.ds.get_schema()
        self.count = data['hits']        
        self.vis['counter'].text = 'Showing {0} out of {1} documents'.format(self.count, schema['__global']['size'])
        '''
        too_big = (self.count <= 1000 and self.big)
        too_small = (self.count > 1000 and not self.big)        
        if too_big or too_small:
            self._rebuild_timelines()
        '''
        self.session.store_document(self.document)    

    def _controls(self):
        def select_change(obj, attr, old, new):
            self.query[obj._owner] = new
            self._update_data()
    
        schema = self.ds.get_schema()
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

        def search_change(obj, attr, old, new):
            self.query[obj._owner] = new
            self._update_data()

        subset = {k:v for k,v in schema.iteritems() if not k == '__global' and v['type'] == 'text'}
        searches = []
        for k,v in subset.iteritems():
            ip = TextInput(
                name=k,
                title=k,
                value=''
            )
            ip._owner = k
            ip.on_change('value', search_change)
            searches.append(ip)

        return VBox(children=selects+searches)
        
    def _resolve_glyph(self):
        factory = None
        if self.count > 1000:
            self.big = True
            def __factory(top, color='black', alpha=0.5):                
                return Quad(left='left',
                            right='right',
                            bottom=0,
                            top=top,
                            line_color=color,
                            line_alpha=alpha)
            factory = __factory
        else:
            self.big = False
            def __factory(null, color='black', alpha=0.5):
                return Circle(x='date',
                              y=1,
                              size=5,
                              fill_color=color,
                              fill_alpha=alpha)
            factory = __factory
        return factory
        
    def _timelines_info(self):
        counter = self.vis['counter']
        schema = self.ds.get_schema()
        counter.text = 'Showing {0} out of {1} documents'.format(self.count, schema['__global']['size'])
        return counter
    
    def _timeline(self, idx, xdr, ydr, glyph_factory):
        column = self.ds.date_columns[idx]
        plot = Plot(data_sources=[self.timeline_source], 
                    x_range=xdr, 
                    y_range=ydr, 
                    title=None,
                    plot_width=675,
                    plot_height=100,
                    min_border=5,
                    min_border_left=50,
                    min_border_bottom=25,
                    min_border_right=1)        

        color = self.colors[idx]
        glyph = glyph_factory(column, color=color, alpha=0.5)
        _renderer = Glyph(data_source=self.timeline_source, 
                          xdata_range=xdr, 
                          ydata_range=ydr, 
                          glyph=glyph)
        plot.renderers.append(_renderer)
        background = Quad(left='left',
                          right='right',
                          bottom=0,
                          top='max',
                          line_color=color,
                          line_alpha='{0}_density'.format(column))             
        bg_renderer = Glyph(data_source=self.timeline_source, 
                            xdata_range=xdr, 
                            ydata_range=ydr, 
                            glyph=background)
        plot.renderers.append(bg_renderer)
        
        formatter = DatetimeTickFormatter(formats=dict(years=['%Y'], 
                                                       months=['%Y-%m'], 
                                                       days=['%Y-%m-%d']))
        xaxis = DatetimeAxis(plot=plot, 
                             location='bottom',
                             dimension=0,
                             formatter=formatter)
        Grid(plot=plot,
             dimension=0,
             axis=xaxis)
        yaxis = LinearAxis(plot=plot, 
                           location='left',
                           dimension=1,
                           major_label_text_font_size="6pt")
        Grid(plot=plot,
             dimension=1,
             axis=yaxis)
        hover = HoverTool(plot=plot, 
                          tooltips=OrderedDict([
                              ("Date", "@date")
                          ]),
                          always_active=True)
        plot.tools.append(hover)
        return plot

    def _timeline_slider(self, idx, xdr, ydr, onchange):
        column = self.ds.date_columns[idx]
        slider = DateRangeSlider(bounds=[datetime.fromtimestamp(d) for d in self.ds.domain],
                                 value=[datetime.fromtimestamp(d) for d in self.ds.domain],
                                 range=(dict(days=1), None),
                                 title=column,
                                 name=column)
        slider._owner = column
        slider.on_change('value', onchange)
        return slider
        

    def _timelines(self):        
        def slide_change(obj, attr, old, new):
            if isinstance(new[0], float):
                return
            else:
                dates = [t.split('T')[0] for t in new]
                q = {
                    'from': dates[0],
                    'to': dates[1]
                }
                self.query[obj._owner] = q        
                self._update_data()

        xdr, ydr = self._make_ranges()
        glyph_factory = self._resolve_glyph()
        timelines = []
        for i in range(len(self.ds.date_columns)):
            slider = self._timeline_slider(i, xdr, ydr, slide_change)
            plot = self._timeline(i, xdr, ydr, glyph_factory)
            timelines.append(VBox(children=[plot, slider]))

        info = self._timelines_info()
        container = VBox(children=[info]+timelines)
        self.vis['timelines']['container'] = container
        return container

    def _rebuild_timelines(self):
        layout = deepcopy(self.vis['layout'])
        layout.children[1] = self._timelines()
        
        document = Document()
        document.add(layout)        

        self.document = document
        self.session.load_document(self.document)        
        self.session.store_document(self.document)
            
    def _documents(self):
        # TODO not arbitrary
        documents = []
        for row in self.ds.get_data()['data']['show']:
            documents.append(PreText(text=row))
        #return VBox(children=documents)
        return VBox(children=[])

    def _make_vis(self):
        layout = HBox(children=[self._controls(),
                                self._timelines(),
                                self._documents()])
        self.vis['layout'] = layout
        self.layout = layout
        self.document.add(layout)        
        self.session.store_document(self.document)    
