'''
Created on Aug 2, 2022

@author: nichamon
'''

import argparse
import os
import sys

# Add the parent directory to sys.path
# sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from LDMS_Test import ContainerTest

class MyExampleTest(ContainerTest):

    test_name = "example"
    test_suite = "example"
    test_type = "FVT"
    test_desc = "This is an example of how to implement a test that is intended " \
                "to be run inside a container."

    # The hostname of the container node to run the test script
    test_node_name = "node-1"

    # The spec object.
    spec = {
        "type" : "NA",
        "nodes" : [
            {
                "hostname" : "node-1", # This is the node the script will be executed on.
                "daemons" : [
                    {
                        "name" : "sshd",
                        "type" : "sshd"
                    },
                    {
                        "name" : "sampler-daemon",
                        "type" : "ldmsd",
                        "listen" : [
                            { "port" : 10001, "xprt" : "sock" }
                        ],
                        "samplers" : [
                            {
                                "plugin" : "meminfo",
                                "config" : [
                                    "component_id=1",
                                    "instance=%hostname%/%plugin%",
                                    "producer=%hostname%"
                                ],
                                "interval" : 1000000,
                                "start" : True
                            }
                        ]
                    }
                ]
            }
        ]
    }

    # List all assertions as the list of (assertion number, assertion's description)
    assertions = [
        (1, "passed"),
        (2, "skipped"),
        (3, "failed")
    ]

def main():
    ap = argparse.ArgumentParser()
    # ContainerTest.add_common_args(..) must be called.
    ContainerTest.add_common_args(ap)
    args = ap.parse_args() 

# Create a ContainerTest object, called suite. We also assign the outdir.
# The outdir is the directory that contains the result file.
    suite = MyExampleTest(args.outdir)

# ---------------------------------------------
    # Test assertion 1
    result_1 = (1 == 1)
    # Save the result
    suite.save_assertion(1, result_1, "1 == 1") # 1 == 1 is the condition we tested.
# ---------------------------------------------
    # Test and save assertion 3
    suite.save_assertion(3, 1 == 2, "1 == 2")

if __name__ == '__main__':
    main()
