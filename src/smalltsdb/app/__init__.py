import sqlite3

from bokeh.embed import components
from bokeh.models import ColumnDataSource
from bokeh.palettes import RdYlBu4
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


def graph_p50(tsdb):
    p = figure(x_axis_type='datetime', title='p50')

    def metric_to_dict(name):
        pairs = tsdb.get_metric(name, 'tensecond', 'p50', (0, 100))
        lists = list(zip(*pairs))
        return {'timestamp': lists[0], 'values': lists[1]}

    p.line(
        x='timestamp',
        y='values',
        source=metric_to_dict('one'),
        legend='one',
        line_width=2,
        line_color=RdYlBu4[0],
    )
    p.line(
        x='timestamp',
        y='values',
        source=metric_to_dict('two'),
        legend='two',
        line_width=2,
        line_color=RdYlBu4[1],
    )

    return p


@blueprint.route('/')
def main():
    plot = graph_p50(get_db())
    script, div = components(plot)
    return render_template('main.html', resources=CDN, script=script, div=div)


def create_app(db_path):
    app = Flask(__name__)
    app.config['SMALLTSDB_DB'] = db_path

    app.secret_key = 'secret'
    app.teardown_appcontext(close_db)

    app.register_blueprint(blueprint)
    return app
