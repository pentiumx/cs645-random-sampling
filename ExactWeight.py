import pandas

class ExactWeight:

    def __init__(self, C=1):
        self.C = C


def main():

    cls = ExactWeight()

    customer_table = pandas.read_table('data_self_generated/customer.tbl', delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)

    customer_table.set_index('NAME', inplace=True)

    # customer_table.loc[1]

    orders_table = pandas.read_table('data_self_generated/orders.tbl', delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    orders_table.set_index('ORDERKEY', inplace=True)



    lineitem_table = pandas.read_table('data_self_generated/lineitem.tbl', delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)

    lineitem_table.set_index(['ORDERKEY'], inplace=True)

    pass


if __name__ == '__main__':
    main()


# http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.3.pdf
#Refer above for schema
# Query 3:
#
# SELECT c_custkey, o_orderkey, l_linenumber
# FROM customer, orders, lineitem
# WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey;

# Query X:
#
# SELECT nationkey, s_suppkey, c_custkey, o_orderkey, l_linenumber
# FROM nation, supplier, customer, orders, lineitem
# WHERE n_nationkey = s_nationkey AND s_nationkey = c_nationkey AND c_custkey = o_custkey AND o_orderkey = l_orderkey;

# Query Y:
#
# SELECT l1.l_linenumber, o1.o_orderkey, c1.c_custkey,
# l2.l_linenumber, o2.o_orderkey, s_suppkey,
# c2.c_custkey
# FROM lineitem l1, orders o1, customer c1,
# lineitem l2, orders o2, customer c2, supplier s
# WHERE
#     l1.l_orderkey = o1.o_orderkey
# AND o1.o_custkey = c1.c_custkey
# AND l1.l_partkey = l2.l_partkey
# AND l2.l_orderkey = o2.o_orderkey
# AND o2.o_custkey = c2.c_custkey
# AND c1.c_nationkey = s.s_nationkey
# AND s.s_nationkey = c2.c_nationkey;


# Query T: (Triangular social graph query)
#
# SELECT * FROM
# popular-user A, twitter-user B, twitter-user C
# WHERE A.dst = B.src
# AND B.dst = C.src AND C.dst = A.src;
#
# Query S: (Triangular social graph query)
#
# SELECT *
# FROM popular-user A, twitter-user B,
# twitter-user C, twitter-user D
# WHERE A.dst = B.src
# AND B.dst = C.src
# AND C.dst = D.src
# AND D.dst = A.src;

# Query F: (Social graph)
#
# SELECT *
# FROM popular-user A, twitter-user B, twitter-user C, popular-user D
# WHERE A.src = B.src
# AND C.dst = A.src
# AND C.src = D.src;

#
# Part table
# Primary Key: P_PARTKEY
#
# SUPPLIER Table
# S_NATIONKEY Identifier Foreign Key to N_NATIONKEY
# Primary Key: S_SUPPKEY
#
# PARTSUPP Table
# PS_PARTKEY Identifier Foreign Key to P_PARTKEY
# PS_SUPPKEY Identifier Foreign Key to S_SUPPKEY
# Primary Key: PS_PARTKEY, PS_SUPPKEY
#
# CUSTOMER Table
# C_NATIONKEY Identifier Foreign Key to N_NATIONKEY
# Primary Key: C_CUSTKEY
#
# ORDERS Table
# O_CUSTKEY Identifier Foreign Key to C_CUSTKEY
# Primary Key: O_ORDERKEY
#
# LINEITEM Table
# L_ORDERKEY identifier Foreign Key to O_ORDERKEY

# L_PARTKEY identifier Foreign key to P_PARTKEY, first part of the
# compound Foreign Key to (PS_PARTKEY,
# PS_SUPPKEY) with L_SUPPKEY

# L_SUPPKEY Identifier Foreign key to S_SUPPKEY, second part of the
# compound Foreign Key to (PS_PARTKEY,
# PS_SUPPKEY) with L_PARTKEY

# Primary Key: L_ORDERKEY, L_LINENUMBER
#
# NATION Table
# N_REGIONKEY identifier Foreign Key to R_REGIONKEY
# Primary Key: N_NATIONKEY
#
# REGION Table
# Primary Key: R_REGIONKEY



# To drop last column
# customer_table = customer_table.iloc[:, :-1]