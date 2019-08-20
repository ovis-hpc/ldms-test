#!/usr/bin/python

import os
import sys
import argparse

from LDMS_Test import parse_ldms_ls

out = """\
Schema         Instance                 Flags  Msize  Dsize  UID    GID    Perm       Update            Duration          Info
-------------- ------------------------ ------ ------ ------ ------ ------ ---------- ----------------- ----------------- --------
syspapi-1      compute-1/syspapi           CL     432    200      0      0 -rwxrwxrwx 1566328023.001436          0.000045 "updt_hint_us"="1000000:0"
meminfo        compute-1/meminfo           CL    2032    432      0      0 -rwxrwxrwx 1566328023.001611          0.000155 "updt_hint_us"="1000000:0"
-------------- ------------------------ ------ ------ ------ ------ ------ ---------- ----------------- ----------------- --------
Total Sets: 2, Meta Data (kB): 2.46, Data (kB) 0.63, Memory (kB): 3.10

=======================================================================

compute-1/meminfo: consistent, last update: Tue Aug 20 19:07:03 2019 +0000 [1611us]
M u64        component_id                               10001
D u64        job_id                                     0
D u64        app_id                                     0
D u64        MemTotal                                   20389036
D u64        MemFree                                    3125536
D u64        MemAvailable                               18922896
D u64        Buffers                                    3168
D u64        Cached                                     15735232
D u64        SwapCached                                 0
D u64        Active                                     871864
D u64        Inactive                                   15136752
D u64        Active(anon)                               212696
D u64        Inactive(anon)                             197564
D u64        Active(file)                               659168
D u64        Inactive(file)                             14939188
D u64        Unevictable                                0
D u64        Mlocked                                    0
D u64        SwapTotal                                  7815164
D u64        SwapFree                                   7815164
D u64        Dirty                                      88
D u64        Writeback                                  0
D u64        AnonPages                                  270276
D u64        Mapped                                     78556
D u64        Shmem                                      140044
D u64        Slab                                       697112
D u64        SReclaimable                               553416
D u64        SUnreclaim                                 143696
D u64        KernelStack                                4400
D u64        PageTables                                 6268
D u64        NFS_Unstable                               0
D u64        Bounce                                     0
D u64        WritebackTmp                               0
D u64        CommitLimit                                18009680
D u64        Committed_AS                               1275436
D u64        VmallocTotal                               34359738367
D u64        VmallocUsed                                330652
D u64        VmallocChunk                               34358947836
D u64        HardwareCorrupted                          0
D u64        AnonHugePages                              28672
D u64        CmaTotal                                   0
D u64        CmaFree                                    0
D u64        HugePages_Total                            0
D u64        HugePages_Free                             0
D u64        HugePages_Rsvd                             0
D u64        HugePages_Surp                             0
D u64        Hugepagesize                               2048
D u64        DirectMap4k                                289408
D u64        DirectMap2M                                20676608

compute-1/syspapi: consistent, last update: Tue Aug 20 19:07:03 2019 +0000 [1436us]
M u64        component_id                               10001
D u64        job_id                                     0
D u64        app_id                                     0
D u64[]      PAPI_TOT_CYC                               358379530,532189836,131486513,184383301
D u64[]      PAPI_TOT_INS                               289414079,457705017,117921960,143886283
D u64[]      PAPI_L1_DCH                                0,0,0,0
D u64[]      PAPI_L1_DCA                                125122783,192286198,45724295,58222420
"""

sets = parse_ldms_ls(out)
assert(set(sets) == set(["compute-1/syspapi", "compute-1/meminfo"]))
meminfo = sets['compute-1/meminfo']
syspapi = sets['compute-1/syspapi']

syspapi_expect = {
  "meta": {
    "info": "\"updt_hint_us\"=\"1000000:0\"",
    "uid": "0",
    "meta_sz": "432",
    "update": "1566328023.001436",
    "perm": "-rwxrwxrwx",
    "instance": "compute-1/syspapi",
    "gid": "0",
    "flags": "CL",
    "duration": "0.000045",
    "data_sz": "200",
    "schema": "syspapi-1"
  },
  "data": {
    "PAPI_L1_DCA": [
      125122783,
      192286198,
      45724295,
      58222420
    ],
    "component_id": 10001,
    "job_id": 0,
    "PAPI_L1_DCH": [
      0,
      0,
      0,
      0
    ],
    "app_id": 0,
    "PAPI_TOT_INS": [
      289414079,
      457705017,
      117921960,
      143886283
    ],
    "PAPI_TOT_CYC": [
      358379530,
      532189836,
      131486513,
      184383301
    ]
  },
  "name": "compute-1/syspapi",
  "ts": "Tue Aug 20 19:07:03 2019 +0000 [1436us]"
}

assert(syspapi == syspapi_expect)

meminfo_expect = {
  "meta": {
    "info": "\"updt_hint_us\"=\"1000000:0\"",
    "uid": "0",
    "meta_sz": "2032",
    "update": "1566328023.001611",
    "perm": "-rwxrwxrwx",
    "instance": "compute-1/meminfo",
    "gid": "0",
    "flags": "CL",
    "duration": "0.000155",
    "data_sz": "432",
    "schema": "meminfo"
  },
  "data": {
    "WritebackTmp": 0,
    "SwapTotal": 7815164,
    "Active(anon)": 212696,
    "SwapFree": 7815164,
    "DirectMap4k": 289408,
    "app_id": 0,
    "KernelStack": 4400,
    "MemFree": 3125536,
    "HugePages_Rsvd": 0,
    "Committed_AS": 1275436,
    "Active(file)": 659168,
    "NFS_Unstable": 0,
    "VmallocChunk": 34358947836,
    "CmaFree": 0,
    "Writeback": 0,
    "Inactive(file)": 14939188,
    "MemTotal": 20389036,
    "job_id": 0,
    "VmallocUsed": 330652,
    "HugePages_Free": 0,
    "AnonHugePages": 28672,
    "AnonPages": 270276,
    "Active": 871864,
    "Inactive(anon)": 197564,
    "CommitLimit": 18009680,
    "Hugepagesize": 2048,
    "Cached": 15735232,
    "SwapCached": 0,
    "VmallocTotal": 34359738367,
    "CmaTotal": 0,
    "component_id": 10001,
    "Dirty": 88,
    "Mapped": 78556,
    "SUnreclaim": 143696,
    "Unevictable": 0,
    "SReclaimable": 553416,
    "MemAvailable": 18922896,
    "Slab": 697112,
    "DirectMap2M": 20676608,
    "HugePages_Surp": 0,
    "Bounce": 0,
    "Inactive": 15136752,
    "PageTables": 6268,
    "HardwareCorrupted": 0,
    "HugePages_Total": 0,
    "Mlocked": 0,
    "Buffers": 3168,
    "Shmem": 140044
  },
  "name": "compute-1/meminfo",
  "ts": "Tue Aug 20 19:07:03 2019 +0000 [1611us]"
}
assert(meminfo == meminfo_expect)
