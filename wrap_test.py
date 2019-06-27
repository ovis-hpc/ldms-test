#!/usr/bin/env python

import os
import sys
from LDMS_Test import Wrapper

_pystart = os.getenv('PYTHONSTARTUP')
if _pystart:
    execfile(_pystart)

class num_list(Wrapper):
    def avg(self):
        return sum(map(float, self.obj)) / len(self.obj)

x = num_list([1,2,3])
print x.avg()
