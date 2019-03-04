#!/usr/bin/env python3

# =============================================================================
#  UNIS-RT
#
#  Copyright (c) 2012-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================

import argparse
import os
import sys
import unittest
import logging

from lace.logging import trace

UNIT_TEST_MODULES = [
    #'unis.test.rest.ProxyTest',
    #'unis.test.rest.ClientTest',
    'unis.test.models.UnisObjectTest',
    'unis.test.models.NetworkResourceTest',
    'unis.test.models.CollectionTest',
    #'unis.test.services.RuntimeServiceTest',
    #'unis.test.runtime.OALTest',
    #'unis.test.runtime.RuntimeTest',
    'unis.test.utils.IndexTest',
    'unis.test.utils.UniqueIndexTest'
]

INTEGRATION_TEST_MODULES = []
TRACELOG = "test_trace.log"
try: os.remove(TRACELOG)
except: pass

trace.enabled(True)
trace.showReturn(True)
trace.showCallDepth(True)
logger = logging.getLogger("unis")
logger.setLevel(5)
hdl = logging.FileHandler(TRACELOG)
hdl.setFormatter(logging.Formatter("[{levelname:.2}] {message} > {name}", style="{"))
logger.addHandler(hdl)

def main(integration=False, unit=False):
    #Setting up path names
    PERISCOPE_ROOT = os.path.dirname(os.path.abspath(__file__)) + os.sep
    sys.path.append(PERISCOPE_ROOT)
    test_modules = []
    
    if unit:
        test_modules.extend(UNIT_TEST_MODULES)
    if integration:
        test_modules.extend(INTEGRATION_TEST_MODULES)

    tsuite = unittest.defaultTestLoader.loadTestsFromNames(test_modules)
    runner = unittest.TextTestRunner()
    ret = not runner.run(tsuite).wasSuccessful()
    
    sys.exit(ret)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", help="Choose which tests to run", type=str,
                        choices=["all", "unit", "integration"], default="unit")
    args = parser.parse_args()

    integration = args.test in ['all', 'integration']
    unit = args.test in ['all', 'unit']
    main(integration, unit)
