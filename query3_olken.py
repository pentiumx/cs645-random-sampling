import random, time
import pandas


def load_tables_q3(data_dir):

    customer_table = pandas.read_table('%s/customer.tbl'%(str(data_dir)+'_tbl'), delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)
    customer_table.set_index('NAME', inplace=True)
    orders_table = pandas.read_table('%s/orders.tbl'%(str(data_dir)+'_tbl'), delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    orders_table.set_index('ORDERKEY', inplace=True)
    lineitem_table = pandas.read_table('%s/lineitem.tbl'%(str(data_dir)+'_tbl'), delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)
    lineitem_table.set_index(['PARTKEY'], inplace=True)
    return customer_table, orders_table, lineitem_table


def semi_join(R1, R2, attr):
    return R1[ R1[attr].isin(R2[attr]) ] # assuming that attr is the index in the previous relation


def W_r0(R, A):
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

def main(num_samples, data_dir):
    DEBUG = False
    c_table, o_table, l_table = load_tables_q3(data_dir)
    R = [c_table, o_table, l_table]     # list of tables we are joining
    A = ['CUSTKEY', 'ORDERKEY']         # list of join attributes
    samples = []
    num_relations = len(R)
    start = time.time()

    wps = []
    w_sjs = []

    # Compute W(t) beforehands
    for i in range(num_relations-1):
        if i == 0:
            wp = W_r0(R, A)
        else:
            wp = W_t(R, i-1, A)

        if i == 0:
            sj = R[0] # r0 joins with all the tuples in R1
        else:
            #sj = semi_join(R[i-1], R[i], A[i])
            sj = semi_join(R[i-1], R[i], A[i-1])
            #sj = semi_join(R[i+1], R[i], A[i-1])
        w_sj = W_t(R, i+1, A)

        wps.append(wp)
        w_sjs.append(w_sj)

    # Proceed with sampling based on Algorithm 1
    for s in range(num_samples):
        S = []
        for i in range(num_relations-1):
            wp = wps[i]
            w_sj = w_sjs[i]

            w = len(sj) * w_sj
            prob = 1.0 - w / wp
            if DEBUG:
                print('-'*100)
                print(A[i])
                print(wp)
                print(w)
                print(prob)

            # reject with the compudated probability
            if random.random() > prob:
                break

            # Sample a tuple t from the semi-joined relations
            t = sj.sample(n=1)
            S.append(t)
        if S != []:
            samples.append(S)

    end = time.time()
    print('Time elapsed: %f seconds' % (end - start))
    print(len(samples))

if __name__ == '__main__':
    samples = [1000000]
    data_dirs = ["0.1", "0.3","0.5", "0.7", "0.9","1.0"]
    for sample in samples:
        print("Running for %d samples"%sample)
        for data_dir in data_dirs:
            print("Running for "+data_dir)
            main(sample, data_dir)
