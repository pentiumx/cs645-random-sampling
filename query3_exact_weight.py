import pandas
import numpy as np
import time
import random


class ExactWeight:

    def __init__(self, C=1):
        self.C = C

    def algorithm1(self, relationsToJoin, total_samples):

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
                for tuple_j in range(len(relationsToJoin[i])):
                    if i == 1:
                        if relationsToJoin[i].iloc[tuple_j]['ORDERKEY'] in relationsToJoin[i+1].index:
                            # Get indices of tuples in i+1 relation which joins with tuple_j
                            indices_of_join_relations = relationsToJoin[i + 1].index.get_loc(relationsToJoin[i].iloc[tuple_j]['ORDERKEY'])

                            # tuple_j_semi_join_R_i_plus_1 = relationsToJoin[i + 1].loc[relationsToJoin[i].iloc[tuple_j].name]

                            if isinstance(indices_of_join_relations, slice):
                                # Include start and stop of the slice.
                                W_t[i][tuple_j] = np.sum(map(W_t[i + 1].get,
                                                             range(indices_of_join_relations.start,
                                                                   indices_of_join_relations.stop)))
                                # for idx in range(indices_of_join_relations.start, indices_of_join_relations.stop):
                                #     W_t[i][tuple_j] += W_t[i + 1][idx]
                            else:
                                if isinstance(indices_of_join_relations, tuple):
                                    W_t[i][tuple_j] = np.sum(map(W_t[i + 1].get, indices_of_join_relations[0]))
                                else:
                                    # single value is returned if only 1 exists
                                    W_t[i][tuple_j] = W_t[i + 1][indices_of_join_relations]
                    if i == 0:
                        # get rows in (i+1)^th relation (orders) which have
                        # relationsToJoin[i].iloc[tuple_j].name
                        # in o_custkey

                        if relationsToJoin[i].iloc[tuple_j].name in relationsToJoin[i + 1].index:
                            indices_of_join_relations = np.where(relationsToJoin[i + 1].index
                                                                 .get_loc(relationsToJoin[i].iloc[tuple_j].name))

                            if isinstance(indices_of_join_relations, slice):
                                W_t[i][tuple_j] = np.sum(map(W_t[i + 1].get,
                                                             range(indices_of_join_relations.start,
                                                                   indices_of_join_relations.stop)))
                                # Include start and stop of the slice.
                                # for idx in range(indices_of_join_relations.start, indices_of_join_relations.stop):
                                #     W_t[i][tuple_j] += W_t[i + 1][idx]
                            else:
                                if isinstance(indices_of_join_relations, tuple):
                                    W_t[i][tuple_j] = np.sum(map(W_t[i + 1].get, indices_of_join_relations[0]))
                                    # for idx in indices_of_join_relations[0]:
                                    #     W_t[i][tuple_j] += W_t[i + 1][idx]
                                # single value is returned if only 1 exists
                                else:
                                    W_t[i][tuple_j] = W_t[i + 1][indices_of_join_relations]

                        # matchingIndexes = relationsToJoin[i + 1].loc[relationsToJoin[i + 1]['CUSTKEY'] == relationsToJoin[i]
                        #         .iloc[tuple_j].name].index

                        # for idx in matchingIndexes:
                        #     row = relationsToJoin[i + 1].index.get_loc(idx)
                        #     # https://stackoverflow.com/questions/34897014/how-do-i-find-the-iloc-of-a-row-in-pandas-dataframe
                        #     W_t[i][tuple_j] += W_t[i + 1][row]
                    pass

            # Query X
            elif num_relations == 5:
                for tuple_j in range(len(relationsToJoin[i])):

                    pass

            elif num_relations == 7:
                for tuple_j in range(len(relationsToJoin[i])):

                    pass


        print("--- %s seconds ---" % (time.time() - start_time))

        # For iterating over dataframe rows
        # for index, row in relationsToJoin[i].iterrows():

        for num_sample in range(total_samples):

            # W_R0 = sum(W_t[0].values())
            idx = self.getRandomKey(W_t[0])
            tuple_t = relationsToJoin[0].iloc[idx]
            for i in range(1, num_relations):
                #Query 3
                if num_relations == 3:
                    if i == 1:
                        indices_of_join_relations = np.where(relationsToJoin[i].index.get_loc(tuple_t.name))
                        if isinstance(indices_of_join_relations, slice):
                            W_t[i][tuple_j] = np.sum(map(W_t[i + 1].get,
                                                         range(indices_of_join_relations.start,
                                                               indices_of_join_relations.stop)))

                        else:
                            if isinstance(indices_of_join_relations, tuple):
                                idx = random.choice(indices_of_join_relations[0])
                                #idx = random.choice([x for x in indices_of_join_relations[0] for y in range(W_t[i][x])])
                                tuple_t = relationsToJoin[i].iloc[idx]
                            else:
                                W_t[i][tuple_j] = W_t[i + 1][indices_of_join_relations]
                    if i == 2:
                        indices_of_join_relations = relationsToJoin[i].index.get_loc(tuple_t['ORDERKEY'])
                        if isinstance(indices_of_join_relations, slice):
                            idx = random.choice(range(indices_of_join_relations.start,
                                                                  indices_of_join_relations.stop))

                            # idx = random.choice([x for x in range(indices_of_join_relations.start,
                            #                                       indices_of_join_relations.stop) for y in range(W_t[i][x])])
                            tuple_t = relationsToJoin[i].iloc[idx]

                        else:
                            if isinstance(indices_of_join_relations, tuple):
                                idx = random.choice(indices_of_join_relations[0])
                                # idx = random.choice([x for x in indices_of_join_relations[0] for y in range(W_t[i][x])])
                                tuple_t = relationsToJoin[i].iloc[idx]
                            else:
                                # Single case
                                tuple_t = relationsToJoin[i].iloc[indices_of_join_relations]

        print("--- %s seconds ---" % (time.time() - start_time))

    def getRandomKey(self, dictionary):
        # Return random key with non-zero value
        filtered_dict = {key: value for key, value in dictionary.items() if value}
        return random.choice(filtered_dict.keys())

        # Too slow
        #random.choice([x for x in dictionary for y in range(dictionary[x])])


def main():
    print("Starting")
    # Query 3: In the order of the join chain
    samples = [1000, 10000,100000,1000000]
    comp = ["0.1", "0.3","0.5", "0.7", "0.9","1.0"]
    for sample in samples:
        print('Doing it for %d samples'%(sample))
        for c in comp:
            print('Doing it for comp: '+str(c))
            cls = ExactWeight()
            customer_table = pandas.read_table(str(c)+'_tbl/customer.tbl', delimiter="|", header=None,
                                               names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                                      "MKTSEGMENT", "COMMENT"], index_col=False)

            customer_table.set_index('CUSTKEY', inplace=True)

            # customer_table.loc[1]
            # relationsToJoin[i].iloc[20].name to get the index value at row 20.

            orders_table = pandas.read_table(str(c)+'_tbl/orders.tbl', delimiter="|", header=None,
                                             names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                                    "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

            orders_table.set_index('CUSTKEY', inplace=True)



            lineitem_table = pandas.read_table(str(c)+'_tbl/lineitem.tbl', delimiter="|", header=None,
                                               names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                                      "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                                      "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                                      "COMMENT"], index_col=False)

            lineitem_table.set_index(['ORDERKEY'], inplace=True)
            cls.algorithm1([customer_table, orders_table, lineitem_table], sample)

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
