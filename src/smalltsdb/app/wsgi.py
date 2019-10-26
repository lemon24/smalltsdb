"""

To run a local development server:

    FLASK_DEBUG=1 FLASK_TRAP_BAD_REQUEST_ERRORS=1 \
    FLASK_APP=src/smalltsdb/app/wsgi.py \
    SMALLTSDB_DB=db.sqlite flask run -h 0.0.0.0 -p 8000

"""
import os

from smalltsdb.app import create_app

# TODO: take the envvar name from somewhere else
app = create_app(os.environ['SMALLTSDB_DB'])
app.config['TRAP_BAD_REQUEST_ERRORS'] = bool(
    os.environ.get('FLASK_TRAP_BAD_REQUEST_ERRORS', '')
)
