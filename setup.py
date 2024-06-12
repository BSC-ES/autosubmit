#!/usr/bin/env python3

# Copyright 2014 Climate Forecasting Unit, IC3

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from os import path
from setuptools import setup
from setuptools import find_packages

here = path.abspath(path.dirname(__file__))

# Get the version number from the relevant file
with open(path.join(here, 'VERSION')) as f:
    version = f.read().strip()

setup(
    name='autosubmit',
    license='GNU GPL v3',
    platforms=['GNU/Linux Debian'],
    version=version,
    description='Autosubmit is a Python-based workflow manager to create, manage and monitor complex tasks involving different substeps, such as scientific computational experiments. These workflows may involve multiple computing systems for their completion, from HPCs to post-processing clusters or workstations. Autosubmit can orchestrate all the tasks integrating the workflow by managing their dependencies, interfacing with all the platforms involved, and handling eventual errors.',
    long_description=open('README_PIP.md').read(),
    author='Daniel Beltran Mora',
    author_email='daniel.beltran@bsc.es',
    url='http://www.bsc.es/projects/earthscience/autosubmit/',
    download_url='https://earth.bsc.es/wiki/doku.php?id=tools:autosubmit',
    keywords=['climate', 'weather', 'workflow', 'HPC'],
    install_requires=[
        'xlib==0.21',
        'setuptools<=68.2.2',
        'bscearth.utils<=0.5.2',
        'requests<=2.31.0',
        'networkx<=2.6.3',
        'portalocker<=2.7.0',
        'mock<=5.1.0',
        'paramiko<=3.4',
        'pyparsing==3.1.1',
        'matplotlib<=3.8.3',
        'argparse<=1.4.0',
        'packaging<=23.2',
        'ruamel.yaml.clib<=0.2.8',
        'typing_extensions<=4.9.0',
        'typing<=3.7.4.3',
        'psutil<=5.6.1',
        'networkx<=2.6.3',
        'py3dotplus==1.1.0',
        'matplotlib<=3.8.3',
        'numpy<2',
        'ruamel.yaml==0.17.21',
        'rocrate==0.*',
        'autosubmitconfigparser==1.0.67',
        'configparser',
        'pathlib',
        'setproctitle'

    ],
    extras_require={
        ':python_version <= "3.7"':
            [
                'PyNaCl==1.5.0',
                'pythondialog==3.5.3',
                'xlib==0.21',
                'setuptools==68.2.2',
                'cryptography==41.0.5',
                'bscearth.utils==0.5.2',
                'requests==2.31.0',
                'networkx==2.6.3',
                'portalocker==2.7.0',
                'mock==5.1.0',
                'paramiko==3.3.1',
                'matplotlib==3.5.3',
                'python_dateutil==2.8.2',
                'argparse==1.4.0',
                'configobj==5.0.8',
                'packaging==23.2',
                'bcrypt==4.0.1',
                'charset_normalizer==3.3.1',
                'kiwisolver==1.4.5',
                'fonttools==4.43.1',
                'cycler==0.12.1',
                'typing_extensions==4.8.0',
                'psutil==5.6.1',
                'Pygments==2.3.1',
                'coverage==5.0',
                'nose==1.3.7',
                'six==1.12.0',
                'Cython==0.29.6',
                'cffi==1.12.2',
                'py==1.8.0',
                'atomicwrites==1.3.0',
                'attrs==19.1.0',
                'more_itertools==6.0.0',
                'urllib3==1.24.1',
                'idna==2.8',
                'Pillow==6.2.1',
                'numpy==1.17.4',
            ],
    },
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: POSIX :: Linux",
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={'autosubmit': [
        'autosubmit/config/files/autosubmit.conf',
        'autosubmit/config/files/expdef.conf',
        'autosubmit/database/data/autosubmit.sql',
        'README',
        'CHANGELOG',
        'VERSION',
        'LICENSE',
        'docs/autosubmit.pdf'
    ]
    },
    scripts=['bin/autosubmit']
)

