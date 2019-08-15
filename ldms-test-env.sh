#!/bin/bash

add_path() {
        local NAME=$1
        local VAL=$2
        [[ *:${!NAME}:* == *:${VAL}:* ]] ||
                eval "export ${NAME}=\"${!NAME}:${VAL}\""
}

PREFIX=/opt/ldms-test
add_path PATH $PREFIX
