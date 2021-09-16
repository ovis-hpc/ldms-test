#!/usr/bin/python3 -i

import os
import sys

from ovis_ldms import ldms
from list_common import G, SCHEMA, add_set, print_set, print_list, gen_data, \
                   update_set, DirMetricList, verify_set, ARRAY_CARD, \
                   char

ldms.init(16*1024*1024)

x0 = ldms.Xprt(name="sock")
x0.connect(host="localhost", port=411)

x1 = ldms.Xprt(name="sock")
x1.connect(host="localhost", port=412)

d0 = x0.dir()
d1 = x1.dir()

list_sampler = x0.lookup(d0[0].name)
set1 = x1.lookup("node-1/set1")
set3_p = x1.lookup("node-1/set3_p")
set3_c = x1.lookup("node-1/set3_c")

list_sampler.update()
set1.update()
set3_p.update()
set3_c.update()

list_sampler_data = [
        (ldms.V_U64, 1), # component_id
        (ldms.V_U64, 0), # job_id
        (ldms.V_U64, 0), # app_id

        (ldms.V_LIST, [ (ldms.V_CHAR, c) for c in 'abc' ]),
        (ldms.V_LIST, [ (ldms.V_U8,  i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S8, -i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U16,  1000 + i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S16, -1000 - i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U32,  100000 + i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S32, -100000 - i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U64,  200000 + i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S64, -200000 - i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_F32,  i) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_D64,  i) for i in range(3) ]),

        (ldms.V_LIST, [ (ldms.V_CHAR_ARRAY, "a_{}".format(i)) for i in range(3) ]),

        (ldms.V_LIST, [ (ldms.V_U8_ARRAY, [i+b for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S8_ARRAY, [-(i+b) for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U16_ARRAY, [  1000+i+b  for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S16_ARRAY, [-(1000+i+b) for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U32_ARRAY, [  100000+i+b  for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S32_ARRAY, [-(100000+i+b) for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_U64_ARRAY, [  500000+i+b  for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_S64_ARRAY, [-(500000+i+b) for b in range(4)]) for i in range(3) ]),

        (ldms.V_LIST, [ (ldms.V_F32_ARRAY, [i+b for b in range(4)]) for i in range(3) ]),
        (ldms.V_LIST, [ (ldms.V_D64_ARRAY, [1000000+i+b for b in range(4)]) for i in range(3) ]),

        # list-of-list
        (ldms.V_LIST, [
            (ldms.V_LIST, [
                (ldms.V_CHAR, char(b'a'[0] + i)),
                (ldms.V_U8,  i),
                (ldms.V_S8, -i),
                (ldms.V_U16,   1000 + i ),
                (ldms.V_S16, -(1000 + i)),
                (ldms.V_U32,   100000 + i ),
                (ldms.V_S32, -(100000 + i)),
                (ldms.V_U64,   200000 + i ),
                (ldms.V_S64, -(200000 + i)),
                (ldms.V_F32, i),
                (ldms.V_D64, i),

                (ldms.V_CHAR_ARRAY, "a_{}".format(i)),

                (ldms.V_U8_ARRAY, [  i+j  for j in range(4)] ),
                (ldms.V_S8_ARRAY, [-(i+j) for j in range(4)] ),
                (ldms.V_U16_ARRAY, [  1000+i+j  for j in range(4)] ),
                (ldms.V_S16_ARRAY, [-(1000+i+j) for j in range(4)] ),
                (ldms.V_U32_ARRAY, [  100000+i+j  for j in range(4)] ),
                (ldms.V_S32_ARRAY, [-(100000+i+j) for j in range(4)] ),
                (ldms.V_U64_ARRAY, [  500000+i+j  for j in range(4)] ),
                (ldms.V_S64_ARRAY, [-(500000+i+j) for j in range(4)] ),

                (ldms.V_F32_ARRAY, [        i+j for j in range(4)] ),
                (ldms.V_D64_ARRAY, [1000000+i+j for j in range(4)] ),

            ]) for i in range(3)
        ])
    ]
verify_set(list_sampler, list_sampler_data)
verify_set(set1)
verify_set(set3_p)
verify_set(set3_c)
