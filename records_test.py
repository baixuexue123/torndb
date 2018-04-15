#!/usr/bin/env python
# -*- coding: utf-8 -*-
import records

DB_CONNECT_STRING = 'mysql+pymysql://crm:123@localhost:3306/test'

db = records.Database(DB_CONNECT_STRING)

rows = db.query('SELECT COUNT(*) AS c FROM tbl1')
print(rows.scalar())

rows = db.query('SELECT * FROM tbl1')
print(rows.first())
print(rows.all())
# print(rows.one())

with db.transaction() as conn:
    r = conn.insert('INSERT INTO tbl1 (`name`) VALUES (:name)', name='haha')
    print(r)
