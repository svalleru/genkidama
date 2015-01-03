__author__ = 'svalleru'
import sqlite3 as lite
import sys

cluster_data = (
    ('esx.lcoal.1', 'pwd1'),
    ('esx.lcoal.2', 'pwd2'),
    ('esx.lcoal.3', 'pwd3'),
    ('esx.lcoal.4', 'pwd4'),
)
try:
    con = lite.connect('cluster.db')
    with con:
        cur = con.cursor()

        cur.execute("DROP TABLE IF EXISTS cluster")
        cur.execute("CREATE TABLE cluster(host TEXT, pwd TEXT)")
        cur.executemany("INSERT INTO cluster VALUES(?, ?)", cluster_data)
        con.commit()
except lite.Error, e:
    if con:
        con.rollback()
    print "Error %s:" % e.args[0]
    sys.exit(1)

finally:
    if con:
        con.close()

print 'data inserted..'
print 'reading data now..'
con = lite.connect('cluster.db')
con.row_factory = lite.Row  # makes dict cursor
cur = con.cursor()
# cur.execute("DELETE FROM cluster")
new_cluster_data = (
    ('esx.lcoal.4', 'pwd5'),
)
cur.executemany("INSERT INTO cluster VALUES(?, ?)", new_cluster_data)
con.commit()

cur.execute("SELECT * FROM cluster")
rows = cur.fetchall()
print len(rows), ' rows fetched..'
for row in rows:
    print "%s %s" % (row["host"], row["pwd"])

if con:
    con.close()