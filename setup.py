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
from setuptools import setup, Command

import sys, os

NAME="mundus"
PACKAGE=f"lib/{NAME}"
with open(os.path.join(f"{PACKAGE}", "version.py")) as f:
    code = compile(f.read(), "version.py", "exec")
    exec(code)

sys.path.append(".")
if sys.version_info[0] < 3 or sys.version_info[1] < 6:
    print("------------------------------")
    print("Must use python 3.6 or greater", file=sys.stderr)
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
        import pytest
        sys.exit(pytest.main(["-x", "tests", "--cov", "mundus", "--cov-report", "term-missing", "-vv",
                              "--cov-config", ".coveragerc"]))

setup(
    name="mundus",
    version=__version__,
    packages=[f"{NAME}", f"{NAME}.models", f"{NAME}.containers"],
    package_dir={'': 'lib'},
    author="Jeremy Musser",
    author_email="jemusser@gmail.iu.edu",
    license="http://www.apache.org/licenses/LICENSE-2.0",
    install_requires=[
        "requests",
        "jsonschema",
        "bson",
        "pyyaml",
        "aiohttp"
    ],
    cmdclass={'test': tester }
)
