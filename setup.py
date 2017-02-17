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

from __future__ import print_function 
from setuptools import setup
from setuptools import Command

import sys

version = "0.1.dev0"

sys.path.append(".")
if sys.version_info[0] < 3: 
    print("------------------------------")
    print("Must use python 3.0 or greater", file=sys.stderr)
    print("Found python version ", sys.version_info, file=sys.stderr)
    print("Installation aborted", file=sys.stderr)
    print("------------------------------")
    sys.exit()

class tester(Command):
    description = "Run unittests for the program"
    user_options = []
    
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass

    def run(self):
        import unis.test.runtests as tests
        return tests.main()

setup(
    name = "unisrt",
    version = version,
    packages = ["unis", "unis.runtime", "unis.models", "unis.utils", "unis.rest", "unis.services", "unis.test"],
    package_data = { 'unis': ['schemas/*']},
    author = "Miao Zhang, Jeremy Musser",
    author_email="miaozhan@indiana.edu",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    
    install_requires=[
        "validictory>=validictory-0.8.1",
        "pytz",
        "python-dateutil",
        "httplib2",
        "websocket-client",
        "requests",
        "jsonschema",
        "bson"
    ],
    cmdclass={'test': tester },
    #entry_points = {
    #    'console_scripts': [
    #        'nreshell = nreshell:main',
    #        'helm = apps.helm.helm:main',
    #    ]
    #},
)
