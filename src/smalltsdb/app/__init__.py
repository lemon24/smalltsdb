import itertools
import sqlite3
from datetime import datetime
from datetime import timedelta

from bokeh.embed import components
from bokeh.models import BoxZoomTool
from bokeh.models import ColumnDataSource
from bokeh.palettes import Category10_10 as palette
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.resources import INLINE
from flask import abort
from flask import Blueprint
from flask import current_app
from flask import Flask
from flask import g
from flask import render_template

from smalltsdb.tsdb import ViewTSDB


blueprint = Blueprint('reader', __name__)
blueprint.add_app_template_global(CDN.render(), 'resources')


def get_db():
    if not hasattr(g, 'db'):
        # tsdb = ViewTSDB(current_app.config['SMALLTSDB_DB'])
        tsdb = ViewTSDB(':memory:')
        tsdb.insert(
            [
                ('one', 5, 2),
                ('two', 6, 4),
                ('one', 8, 1),
                ('two', 12, 4),
                ('one', 16, 5),
                ('one', 22, 2),
                ('one', 31, 1),
                ('one', 33, 2),
                ('two', 40, 6),
                ('one', 48, 4),
            ]
        )
        g.db = tsdb
    return g.db


def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


def make_graph(tsdb, metrics, interval, width=600, height=200, title=None, label=None):

    plot = figure(
        x_axis_type='datetime',
        toolbar_location='above',
        plot_width=width,
        plot_height=height,
        title=title,
        x_range=interval,
    )

    # TODO: add all tools from the beginning
    wzoom = BoxZoomTool(dimensions="width")
    plot.add_tools(wzoom)
    plot.toolbar.autohide = True
    plot.toolbar.active_drag = None  # bad for mobile otherwise
    # TODO: also active scroll = zoom horizontal

    if label:
        plot.yaxis.axis_label = label

    colors = itertools.cycle(palette)

    for (name, period, stat), color in zip(metrics, colors):
        metric = tsdb.get_metric(name, period, stat, interval)
        lists = list(zip(*metric))
        source = {'timestamp': lists[0], 'values': lists[1]}

        plot.line(
            x='timestamp',
            y='values',
            source=source,
            # TODO: better auto-guessing of names
            legend=name,
            line_width=1.2,
            line_color=color,
        )

    return plot


@blueprint.route('/graph')
def graph():

    # TODO: get start/end from query string
    start = 0
    end = 100

    plot = make_graph(
        get_db(),
        # TODO: metric def from query string
        [('one', 'tensecond', 'avg'), ('two', 'tensecond', 'avg')],
        (start, end),
        title='graph',
        label='things',
    )

    script, div = components(plot)
    return render_template('graphs.html', script=script, divs=[div], title='graph')


@blueprint.route('/')
def metrics():
    metrics = get_db().list_metrics()
    return render_template('metrics.html', metrics=metrics, title='metrics')


def create_app(db_path):
    app = Flask(__name__)
    app.config['SMALLTSDB_DB'] = db_path

    app.secret_key = 'secret'
    app.teardown_appcontext(close_db)

    app.register_blueprint(blueprint)
    return app
