import pandas
import numpy as np
import time
import random


class ExactWeight:

    def __init__(self, C=1):
        self.C = C



    def get_Wt(self, tuple_elem, relation_to_join_to ,wt_i_plus_1):


        retVal = 0

        indices_of_join_relations = relation_to_join_to.index.get_loc(tuple_elem)

        if isinstance(indices_of_join_relations, np.ndarray):
            indices_of_join_relations = np.where(indices_of_join_relations)

        if isinstance(indices_of_join_relations, slice):
            # Include start and stop of the slice.
            retVal = np.sum(map(wt_i_plus_1.get,
                                         range(indices_of_join_relations.start,
                                               indices_of_join_relations.stop)))
            # for idx in range(indices_of_join_relations.start, indices_of_join_relations.stop):
            #     W_t[i][tuple_j] += W_t[i + 1][idx]
        else:
            if isinstance(indices_of_join_relations, tuple):
                retVal = np.sum(map(wt_i_plus_1.get, indices_of_join_relations[0]))
            else:
                # single value is returned if only 1 exists
                retVal = wt_i_plus_1[indices_of_join_relations]

        return retVal

    def algorithm1(self, relationsToJoin):

        num_relations = len(relationsToJoin)
        # For exact weight, W_t needs to be precomputed using DP.
        W_t = []

        for i in range(num_relations):
            W_t.append({})
            # DP initialization
            for tuple_j in range(len(relationsToJoin[i])):
                if (i == num_relations - 1):
                    W_t[i][tuple_j] = 1
                else:
                    W_t[i][tuple_j] = 0

        start_time = time.time()
        for i in range(num_relations-2,-1,-1):
            # Note -1 lower bound is excluded
            print("--- %s seconds ---" % (time.time() - start_time))
            # Query 3
            if num_relations == 3:

                if i == 1:
                    for tuple_j in range(len(relationsToJoin[i])):
                        # print(tuple_j)
                        if relationsToJoin[i].iloc[tuple_j]['ORDERKEY'] in relationsToJoin[i+1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j]['ORDERKEY'],
                                                          relationsToJoin[i + 1], W_t[i + 1])

                if i == 0:
                    print("--- %s seconds ---" % (time.time() - start_time))
                    for tuple_j in range(len(relationsToJoin[i])):
                        # print(tuple_j)
                        if relationsToJoin[i].iloc[tuple_j].name in relationsToJoin[i + 1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j].name,
                                                          relationsToJoin[i + 1], W_t[i + 1])

            # Query X
            elif num_relations == 5:
                if i == 3:
                    for tuple_j in range(len(relationsToJoin[i])):
                        # orders, lineitem
                        if relationsToJoin[i].iloc[tuple_j]['ORDERKEY'] in relationsToJoin[i+1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j]['ORDERKEY'],
                                                          relationsToJoin[i + 1], W_t[i + 1])
                if i == 2:
                    print("--- %s seconds ---" % (time.time() - start_time))
                    for tuple_j in range(len(relationsToJoin[i])):
                        # customers, orders
                        if relationsToJoin[i].iloc[tuple_j]['CUSTKEY'] in relationsToJoin[i + 1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j]['CUSTKEY'],
                                                          relationsToJoin[i + 1], W_t[i + 1])
                if i == 1:
                    print("--- %s seconds ---" % (time.time() - start_time))
                    for tuple_j in range(len(relationsToJoin[i])):
                        # supplier, customer
                        if relationsToJoin[i].iloc[tuple_j].name in relationsToJoin[i + 1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j].name,
                                                          relationsToJoin[i + 1], W_t[i + 1])
                if i == 0:
                    print("--- %s seconds ---" % (time.time() - start_time))
                    for tuple_j in range(len(relationsToJoin[i])):
                        # nation, supplier
                        if relationsToJoin[i].iloc[tuple_j].name in relationsToJoin[i + 1].index:

                            W_t[i][tuple_j] = self.get_Wt(relationsToJoin[i].iloc[tuple_j].name,
                                                          relationsToJoin[i + 1], W_t[i + 1])

            elif num_relations == 7:

                pass


        print("--- %s seconds ---" % (time.time() - start_time))

        setOfNonZeroWt = {}
        for i in range(1, num_relations):
            setOfNonZeroWt[i] = set(self.getIndicesForNonZeroWt(W_t[i]))

        # For iterating over dataframe rows
        # for index, row in relationsToJoin[i].iterrows():
        start_time = time.time()
        total_samples = 1000
        samples = {}
        for num_sample in range(total_samples):
            samples[num_sample] = []
            # W_R0 = sum(W_t[0].values())
            # print(num_sample)
            idx = self.getRandomKey(W_t[0])
            tuple_t = relationsToJoin[0].iloc[idx]
            samples[num_sample].append({relationsToJoin[0].index.name:tuple_t.name})
            samples[num_sample].append(tuple_t)
            for i in range(1, num_relations):
                #Query 3
                if num_relations == 3:
                    if i == 1:
                        indices_of_join_relations = np.where(relationsToJoin[i].index.get_loc(tuple_t.name))
                    if i == 2:
                        indices_of_join_relations = relationsToJoin[i].index.get_loc(tuple_t['ORDERKEY'])

                if num_relations==5:
                    # print(i,' ',tuple_t.name)


                    if i==3:
                        indices_of_join_relations = np.where(relationsToJoin[i].index.get_loc(tuple_t['CUSTKEY']))
                    elif i==4:
                        indices_of_join_relations = np.where(relationsToJoin[i].index.get_loc(tuple_t['ORDERKEY']))
                    else:
                        indices_of_join_relations = np.where(relationsToJoin[i].index.get_loc(tuple_t.name))

                if isinstance(indices_of_join_relations, slice):
                    filteredList = list(set(range(indices_of_join_relations.start,
                          indices_of_join_relations.stop)) & setOfNonZeroWt[i])
                    idx = random.choice(filteredList)

                    # idx = random.choice([x for x in range(indices_of_join_relations.start,
                    #                                       indices_of_join_relations.stop) for y in range(W_t[i][x])])
                    tuple_t = relationsToJoin[i].iloc[idx]

                else:
                    if isinstance(indices_of_join_relations, tuple):
                        filteredList = list(set(indices_of_join_relations[0]) & setOfNonZeroWt[i])
                        idx = random.choice(filteredList)
                        tuple_t = relationsToJoin[i].iloc[idx]
                    else:
                        tuple_t = relationsToJoin[i].iloc[indices_of_join_relations]
                samples[num_sample].append(tuple_t)

        print("--- Sampling ", total_samples,  " samples took %s seconds ---" % (time.time() - start_time))

    def getRandomKey(self, dictionary):
        # Return random key with non-zero value
        return random.choice(self.getIndicesForNonZeroWt(dictionary))

    def algorithm3(self, orders_table, customer_table, lineitem_table, supplier_table):
        order_cust = orders_table.merge(customer_table, left_on='CUSTKEY', right_on='CUSTKEY')
        # order_cust_sup = order_cust.merge(supplier_table, left_on='NATIONKEY', right_on='NATIONKEY')

        #index of lineitem_table is on ORDERKEY
        filtered_lineItems = lineitem_table[lineitem_table.index.isin(order_cust['ORDERKEY'])]


        samples = 10000
        for i in range(samples):
            print(i)
            arbitLineItemIdx = random.choice(range(len(filtered_lineItems)))

            lineItemTuple1 = lineitem_table.iloc[arbitLineItemIdx]
            partkey = lineItemTuple1['PARTKEY']

            filtered_lineItems_on_partkey = lineitem_table[lineitem_table['PARTKEY']==partkey]

            # Will be only 1 tuple
            order_cust_tuple_1_join = order_cust[order_cust['ORDERKEY'] == lineItemTuple1.name]

            filtered_suppliers = supplier_table[supplier_table.index.isin(order_cust_tuple_1_join['NATIONKEY'])]

            order_cust_tuple_2_join_set = order_cust[order_cust['NATIONKEY'] == order_cust_tuple_1_join['NATIONKEY'].values[0]]

            lineItemTuple2 = filtered_lineItems_on_partkey[filtered_lineItems_on_partkey.index.isin(order_cust_tuple_2_join_set['ORDERKEY'])]

            # print('Sample:',lineItemTuple1,lineItemTuple2.iloc[0],order_cust_tuple_1_join.iloc[0],
            #       order_cust_tuple_2_join_set.iloc[0], filtered_suppliers.iloc[0])

        # lineOrderCustMerge1 = lineItemTuple1.merge(order_cust, left_on='ORDERKEY', right_on='ORDERKEY')
        # lineOrderCustMerge2 = lineItemTuple2.merge(order_cust, left_on='ORDERKEY', right_on='ORDERKEY')


        pass


    def getIndicesForNonZeroWt(self, dictionary):
        filtered_dict = {key: value for key, value in dictionary.items() if value > 0}
        # Too slow
        # random.choice([x for x in dictionary for y in range(dictionary[x])])
        return filtered_dict.keys()

    def reverseSampling(self, customer_table, orders_table, relation):
        total_samples = 1000
        start_time = time.time()
        for num_sample in range(total_samples):

            # W_R0 = sum(W_t[0].values())
            # print(num_sample)
            idx = random.randint(0, len(relation))
            tuple_t = relation.iloc[idx]
            order_tuple = orders_table[orders_table['ORDERKEY'] == relation.iloc[idx].name]
            cust_key = order_tuple.index._values[0]
            cust_tuple = customer_table[customer_table.index == cust_key]
        print("--- Reverse Sampling ", total_samples, " samples took %s seconds ---" % (time.time() - start_time))

def main():

    cls = ExactWeight()

    queryNum = input("Which query? 1. Query3, 2. Query X, 3. Query Y. Enter number.")

    print "Starting query : ", queryNum

    if queryNum > 1:

        supplier_table = pandas.read_table('../data_self_generated/sc0.01/supplier.tbl', delimiter="|", header=None,
                                         names=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                                "COMMENT"], index_col=False)

        supplier_table.set_index('NATIONKEY', inplace=True)

    if queryNum == 2:

        nation_table = pandas.read_table('../data_self_generated/sc0.01/nation.tbl', delimiter="|", header=None,
                                           names=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"], index_col=False)

        nation_table.set_index('NATIONKEY', inplace=True)


    customer_table = pandas.read_table('../data_self_generated/sc0.01/customer.tbl', delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)

    if queryNum==1 or queryNum==3:
        customer_table.set_index('CUSTKEY', inplace=True)
    elif queryNum==2:
        customer_table.set_index('NATIONKEY', inplace=True)

    # customer_table.loc[1]
    # relationsToJoin[i].iloc[20].name to get the index value at row 20.

    orders_table = pandas.read_table('../data_self_generated/sc0.01/orders.tbl', delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    orders_table.set_index('CUSTKEY', inplace=True)



    lineitem_table = pandas.read_table('../data_self_generated/sc0.01/lineitem.tbl', delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)

    lineitem_table.set_index(['ORDERKEY'], inplace=True)

    # if queryNum==1:
    #     # Query 3: In the order of the join chain
    #     cls.algorithm1([customer_table, orders_table, lineitem_table])
    #
    # elif queryNum==2:
    #     # Query X: In the order of the join chain
    #     cls.algorithm1([nation_table, supplier_table, customer_table, orders_table, lineitem_table])
    #
    # elif queryNum==3:
    #     # Query Y: In the order of the join chain
    #
    #     # order_cust_supplier_cust_order = order_cust_sup.merge(order_cust, left_on='NATIONKEY', right_on='NATIONKEY')
    #     cls.algorithm3(orders_table, customer_table, lineitem_table, supplier_table)
    #     pass

    # cls.reverseSampling(customer_table, orders_table, lineitem_table)
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

# References:
# https://stackoverflow.com/questions/38542419/could-pandas-use-column-as-index

# https://stackoverflow.com/questions/17071871/select-rows-from-a-dataframe-based-on-values-in-a-column-in-pandas