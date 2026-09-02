#!/bin/bash

# Define TEST_LIST to be used with ldms_test_agent.sh on OCI
source test-list.sh
TEST_LIST=(
	${PAPI_CONT_TEST_LIST[@]}
	${CONT_TEST_LIST[@]}
)
