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

RUN_UNIT_TESTING = True
RUN_INTEGRATION_TESTING = False

UNIT_TEST_MODULES = [
    'unis.test.models.UnisObjectTest',
    'unis.test.models.NetworkResourceTest',
    'unis.test.models.CollectionTest',
    'unis.test.runtime.UnisServiceTest',
    'unis.test.runtime.OALTest'
]

INTEGRATION_TEST_MODULES = []

def main():
    #Setting up path names
    UNIS_ROOT = os.path.dirname(os.path.abspath(__file__)) + os.sep
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(UNIS_ROOT))))
    test_modules = []
    
    if RUN_UNIT_TESTING:
        test_modules.extend(UNIT_TEST_MODULES)
    if RUN_INTEGRATION_TESTING:
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

    if args.test == "all":
        RUN_INTEGRATION_TESTING = True
    elif args.test == "integration":
        RUN_INTEGRATION_TESTING = True
        RUN_UNIT_TESTING = False

    main()
