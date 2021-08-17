#! /usr/bin/env python3

import dbm.gnu

with dbm.gnu.open("test.gdbm","c") as db:
    for i in range(0,100):
        db["+{}+".format(i*2).encode('utf-8')] = "-{}-".format(i*2+1).encode('utf-8')
