#!/usr/bin/python
# Test --debug CLI option
import os
import sys
import logging
import argparse

import TADA
from LDMS_Test import process_args, add_common_args

logging.basicConfig(format = "%(asctime)s %(name)s %(levelname)s %(message)s",
                    level = logging.INFO)

log = logging.getLogger(__name__)

ap = argparse.ArgumentParser(description = "Testing --debug flag")
add_common_args(ap)
args = ap.parse_args()
process_args(args)

test = TADA.Test(test_suite = "Test",
                 test_type = "FVT",
                 test_name = "test_debug",
                 test_desc = "Test debug flag",
                 test_user = args.user,
                 commit_id = 'None',
                 tada_addr = args.tada_addr)
test.add_assertion(1, "Always fail")
test.start()

if args.debug:
    try:
        test.assert_test(1, False, "False")
    except TADA.AssertionException, e:
        log.info("Caught expected TADA.AssertionException")
        pass
    else:
        raise RuntimeError("TADA.AssertionException not raised")
else:
    test.assert_test(1, False, "False")
test.finish()
