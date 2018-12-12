import os
import pandas
import csv, sqlite3
import time

conn = sqlite3.connect(":memory:")
cur = conn.cursor()


customer_table = pandas.read_table('customer.tbl', delimiter="|", header=None,
                                   names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                          "MKTSEGMENT", "COMMENT"], index_col=False)
customer_table.to_sql('customer', conn, if_exists='append', index=False)


orders_table = pandas.read_table('orders.tbl', delimiter="|", header=None,
                                 names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                        "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

orders_table.to_sql('orders', conn, if_exists='append', index=False)

#orders_table.set_index('ORDERKEY', inplace=True)
lineitem_table = pandas.read_table('lineitem.tbl', delimiter="|", header=None,
                                   names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                          "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                          "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                          "COMMENT"], index_col=False)

lineitem_table.to_sql('lineitem', conn, if_exists='append', index=False)



supplier_table = pandas.read_table('supplier.tbl', delimiter="|", header=None,
                                   names=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY",
                                   "PHONE", "ACCTBAL", "COMMENT"]
                                   , index_col=False)

supplier_table.to_sql('supplier', conn, if_exists='append', index=False)


nation_table = pandas.read_table('nation.tbl', delimiter="|", header=None,
                                   names=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"]
                                   , index_col=False)

nation_table.to_sql('nation', conn, if_exists='append', index=False)


print("Trying out query 3")
query3 = "SELECT c.CUSTKEY as c_custkey,o.CUSTKEY as  o_orderkey, l.LINENUMBER as l_linenumber FROM customer c, orders o, lineitem l WHERE c.CUSTKEY = o.CUSTKEY AND l.ORDERKEY = o.ORDERKEY"
print("Running- "+query3)
t_start = time.time()
cur.execute(query3)
print("Time taken is %f seconds"%(time.time()-t_start))

print("Trying out query x")
queryX = "SELECT n.NATIONKEY, s.SUPPKEY, c.CUSTKEY, o.ORDERKEY, l.LINENUMBER FROM nation n, supplier s, customer c, orders o, lineitem l WHERE n.NATIONKEY = s.NATIONKEY AND s.NATIONKEY = c.NATIONKEY AND c.CUSTKEY = o.CUSTKEY AND o.ORDERKEY = l.ORDERKEY"
print("Running- "+queryX)
t_start = time.time()
cur.execute(queryX)
print("Time taken is %f seconds"%(time.time()-t_start))

print("Trying out query y")
queryY = "SELECT l1.LINENUMBER, o1.ORDERKEY, c1.CUSTKEY, l2.LINENUMBER, o2.ORDERKEY, s.SUPPKEY, c2.CUSTKEY FROM lineitem l1, orders o1, customer c1, lineitem l2, orders o2, customer c2, supplier s WHERE l1.ORDERKEY = o1.ORDERKEY AND o1.CUSTKEY = c1.CUSTKEY AND l1.PARTKEY = l2.PARTKEY AND l2.ORDERKEY = o2.ORDERKEY AND o2.CUSTKEY = c2.CUSTKEY AND c1.NATIONKEY = s.NATIONKEY AND s.NATIONKEY = c2.NATIONKEY;"
print("Running- "+queryY)
t_start = time.time()
cur.execute(queryY)
print("Time taken is %f seconds"%(time.time()-t_start))
