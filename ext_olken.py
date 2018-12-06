import random, time
import pandas
from ExactWeight import ExactWeight


def load_tables_q3():

    cls = ExactWeight()

    customer_table = pandas.read_table('data_0.1/customer.tbl', delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)
    #customer_table.set_index('NAME', inplace=True)
    orders_table = pandas.read_table('data_0.1/orders.tbl', delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    #orders_table.set_index('ORDERKEY', inplace=True)
    lineitem_table = pandas.read_table('data_0.1/lineitem.tbl', delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)
    #lineitem_table.set_index(['ORDERKEY'], inplace=True)
    return customer_table, orders_table, lineitem_table


def semi_join(R1, R2, attr):
    return R1[ R1[attr].isin(R2[attr]) ] # assuming that attr is the index in the previous relation


def W_r0(R, A):
    # R: R = [None, c_table, o_table, l_table]
    prod = 1
    n = len(R)
    size = len(R[1])
    for j in range(2, n): # W(r0) = W(R1), then i=1, j=i+1 to n
        M = R[j][A[j-1]].value_counts().max()
        prod *= M
    return prod * size

def W_t(R, i, A):
    # A: join attributes
    # R: relations, i: the current index i, t: a tuple in Ri
    # refer to the equation in Sectin 4.1
    prod = 1
    n = len(R)
    for j in range(i+1, n):
        M = R[j][A[j-1]].value_counts().max()
        prod *= M
    return prod

def main(num_samples):
    DEBUG = False
    c_table, o_table, l_table = load_tables_q3()
    R = [c_table, o_table, l_table]     # list of tables we are joining
    A = ['CUSTKEY', 'ORDERKEY']         # list of join attributes
    samples = []
    num_relations = len(R)
    start = time.time()

    for i in range(num_relations-1):
        r = R[i]
        if i == 0:
            wp = W_r0(R, A)
        else:
            wp = W_t(R, i-1, A)

        if i == 0:
            sj = R[0] # r0 joins with all the tuples in R1
        else:
            sj = semi_join(R[i-1], R[i], A[i-1])

        w_sj = W_t(R, i+1, A)
        w = len(sj) * w_sj
        prob = 1.0 - w / wp
        if DEBUG:
            print('-'*100)
            print(A[i])
            print(wp)
            print(w)
            print(prob)

        # reject with the probability
        if random.random() > prob:
            break

        # Sample a tuple t from the semi-joined relations
        samples = sj.sample(n=num_samples)

    end = time.time()
    print('Time elapsed: %d seconds' % (end - start))
    print(len(samples))


if __name__ == '__main__':
    main(1000)

