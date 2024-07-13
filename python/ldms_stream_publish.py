#!/usr/bin/python3
#
# SYNOPSIS
# --------
# ldms_stream_publish.py - stream publisher script for using in `ldms_stream_test`

import os
import sys

from ovis_ldms import ldms
from ldms_stream_common import *

r = stream_connect(HOSTNAME, rail_eps = 1)
