#!/usr/bin/env python3
import pprint
import time
import torndb

st = time.time()

db = torndb.Connection(host='localhost', database='blog',
                       user='blog', password='blog')

# ret = db.insert("INSERT INTO authors (email, name, hashed_password) VALUES (%s, %s, %s)",
#                 "baixue7@sohu.com", 'baixue7', '123456')
#
# pprint.pprint(ret)  # 返回id


# ret = db.insert("INSERT INTO entries (author_id, slug, title, markdown, html)"
#                 "VALUES (%s, %s, %s, %s, %s)",
#                 3, 'slug4', 'title', 'markdown', 'html')
#
# pprint.pprint(ret)  # 返回id


# ret = db.get("SELECT * FROM authors WHERE email = %s", "baixue3@sohu.com")
#
# pprint.pprint(ret)


# ret = db.query("SELECT * FROM entries WHERE  author_id = %s", 1)
#
# pprint.pprint(ret)


ret = db.query("SELECT entries.*, authors.name as author_name, authors.email as author_email "
               "FROM entries JOIN authors on entries.author_id=authors.id WHERE  author_id=%s", 1)

pprint.pprint(ret)


print(time.time() - st)

db.close()
