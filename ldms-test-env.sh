#!/bin/bash

add_path() {
        local NAME=$1
        local VAL=$2
        [[ *:${!NAME}:* == *:${VAL}:* ]] ||
                eval "export ${NAME}=\"${VAL}:${!NAME}\""
}

PREFIX=/home/narate/projects/ldms-test
add_path PATH $PREFIX
add_path PYTHONPATH $PREFIX
