#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from setuptools import setup

version = "0.1.dev"

setup(
    name = "nre",
    version = version,
    py_modules=['nreshell', 'settings'],
    packages = ["unis", "unis.runtime", "unis.models", "unis.utils", "unis.rest", "unis.services"],
    package_data = { 'unis': ['schemas/*']},
    author = "Miao Zhang, Jeremy Musser",
    author_email="miaozhan@indiana.edu",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    
    install_requires=[
        "validictory>=validictory-0.8.1",
        "pytz",
        "python-dateutil",
        "httplib2",
        "websocket-client"
        #"graph-tool" -- apt-get install
    ],
    
    entry_points = {
        'console_scripts': [
            'nreshell = nreshell:main',
            'helm = apps.helm.helm:main',
        ]
    },
)
