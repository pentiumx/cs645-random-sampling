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
    def reverseSampling(self, relation, sample_size):
        start_time = time.time()
        for num_sample in range(sample_size):
            idx = random.randint(0, len(relation)-1)
            tuple_t = relation.iloc[idx]
        print("--- Reverse Sampling ", sample_size, " samples took %s seconds ---" % (time.time() - start_time))

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
            cls.reverseSampling(lineitem_table, sample)


if __name__ == '__main__':
    main()
