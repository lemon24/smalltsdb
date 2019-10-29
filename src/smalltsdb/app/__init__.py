import itertools
import sqlite3
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import click
import iso8601
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
from flask import request
from jinja2 import StrictUndefined

from smalltsdb.tsdb import PERIODS
from smalltsdb.tsdb import STATS
from smalltsdb.tsdb import TSDB


blueprint = Blueprint('smalltsdb', __name__)
blueprint.add_app_template_global(CDN.render(), 'resources')
blueprint.add_app_template_global(STATS, 'STATS')
blueprint.add_app_template_global(PERIODS, 'PERIODS')


def get_db():
    if not hasattr(g, 'db'):
        tsdb = TSDB(current_app.config['SMALLTSDB_DB'])
        """
        tsdb = TSDB(':memory:')
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
        tsdb.sync()
        """
        g.db = tsdb
    return g.db


def close_db(error):
    if hasattr(g, 'db'):
        g.db.close()


def make_graph(
    tsdb, metrics, interval, width=600, height=200, title=None, label=None, points=False
):

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
        if not lists:
            # need at least 1 value, otherwise the graph looks wonky
            # TODO: should get_metric() always emit the whole time range?
            lists = [[0], [0]]
        # Bokeh treats timestamps as microseconds instead of seconds
        lists[0] = [ts * 1000 for ts in lists[0]]
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

        if points:
            # TODO: deduplicate the arguments
            plot.circle(
                x='timestamp',
                y='values',
                source=source,
                # TODO: better auto-guessing of names
                legend=name,
                line_width=1.2,
                line_color=color,
            )

    # hacky, from
    # https://docs.bokeh.org/en/latest/docs/user_guide/styling.html#outside-the-plot-area
    plot.add_layout(plot.legend[0], 'right')

    # TODO: show/hide legend

    return plot


def parse_datetime(value):
    # TODO: support durations (e.g. -PT3M)
    if isinstance(value, datetime):
        return value
    try:
        return int(value)
    except Exception:
        pass
    return iso8601.parse_date(value)


@blueprint.route('/graph')
def graph():

    # TODO: allow passing in more than one metric
    metric = request.args['metric']
    period = request.args['period']
    stat = request.args['stat']

    default_end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    default_start = default_end - timedelta(hours=1)

    start = parse_datetime(request.args.get('start', default_start)).replace(
        tzinfo=None
    )
    end = parse_datetime(request.args.get('end', default_end)).replace(tzinfo=None)

    title = request.args.get('title')
    label = request.args.get('label')
    # TODO: better bools
    points = request.args.get('points')

    # TODO: show gaps

    # TODO: lower should default to 0
    # TODO: add lower/upper

    plot = make_graph(
        get_db(),
        [(metric, period, stat)],
        (start, end),
        title=title,
        label=label,
        points=points,
    )

    script, div = components(plot)
    return render_template(
        'graphs.html',
        script=script,
        divs=[div],
        title='graph',
        start=start,
        end=end,
        metric=metric,
        period=period,
        stat=stat,
        points=points,
    )


@blueprint.route('/')
def metrics():
    metrics = get_db().list_metrics()
    # other default, maybe?

    return render_template('metrics.html', metrics=metrics, title='metrics')


def create_app(db_path):
    app = Flask(__name__)
    app.config['SMALLTSDB_DB'] = db_path

    app.secret_key = 'secret'
    app.teardown_appcontext(close_db)
    app.jinja_env.undefined = StrictUndefined

    app.register_blueprint(blueprint)
    return app
