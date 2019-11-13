import ast
import re

from setuptools import find_packages
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('src/smalltsdb/__init__.py', 'rb') as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode('utf-8')).group(1))
    )

with open('README.md') as f:
    long_description = f.read()

setup(
    name='smalltsdb',
    version=version,
    author='lemon24',
    url='https://github.com/lemon24/smalltsdb',
    # license='BSD',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=['numpy'],
    extras_require={
        'daemon': ['click'],
        'sync': ['fasteners'],
        'web-app': ['flask', 'bokeh', 'iso8601'],
        'dev': [
            # tests
            'pytest',
            'coverage',
            'pytest-cov',
            'tox',
            'mypy',
            'pytest-randomly',
            # docs
            'sphinx',
            'sphinx_rtd_theme',
            'click',
            'sphinx-click',
            # release
            'twine',
            # ...
            'pre-commit',
        ],
        'docs': ['sphinx', 'sphinx_rtd_theme', 'click', 'sphinx-click'],
    },
    description="A time series database that doesn't scale.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    project_urls={
        # "Documentation": "https://smalltsdb.readthedocs.io/",
        "Code": "https://github.com/lemon24/smalltsdb",
        # "Issue tracker": "https://github.com/lemon24/smalltsdb/issues",
    },
    classifiers=[
        # TODO: More classifiers!
        'Development Status :: 3 - Alpha',
        #'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
