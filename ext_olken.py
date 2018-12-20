import random, time
import pandas

def load_tables_q3():
    data_dir = 'data_0.1'
    customer_table = pandas.read_table('%s/customer.tbl'%data_dir, delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)
    customer_table.set_index('NAME', inplace=True)
    orders_table = pandas.read_table('%s/orders.tbl'%data_dir, delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    orders_table.set_index('ORDERKEY', inplace=True)
    lineitem_table = pandas.read_table('%s/lineitem.tbl'%data_dir, delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)
    lineitem_table.set_index(['PARTKEY'], inplace=True)
    return [customer_table, orders_table, lineitem_table]

def load_tables_qx():
    data_dir = 'data_0.1'
    print("Loading supplier table")
    supplier_table = pandas.read_table('%s/supplier.tbl'%data_dir, delimiter="|", header=None,
                                     names=["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                            "COMMENT"], index_col=False)

    supplier_table.set_index('NAME', inplace=True)
    print("Loading nation table")
    nation_table = pandas.read_table('%s/nation.tbl'%data_dir, delimiter="|", header=None,
                                       names=["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"], index_col=False)

    nation_table.set_index('NAME', inplace=True)
    print("Loading customer table")
    customer_table = pandas.read_table('%s/customer.tbl'%data_dir, delimiter="|", header=None,
                                       names=["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL",
                                              "MKTSEGMENT", "COMMENT"], index_col=False)
    customer_table.set_index('NAME', inplace=True)
    print("Loading orders table")
    orders_table = pandas.read_table('%s/orders.tbl'%data_dir, delimiter="|", header=None,
                                     names=["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE",
                                            "ORDERPRIORITY","CLERK","SHIPPRIORITY","COMMENT"], index_col=False)

    orders_table.set_index('CLERK', inplace=True)
    print("Loading lineitem table")
    lineitem_table = pandas.read_table('%s/lineitem.tbl'%data_dir, delimiter="|", header=None,
                                       names=["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER","QUANTITY",
                                              "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS",
                                              "SHIPDATE","COMMITDATE","RECEIPTDATE","SHIPINSTRUCT","SHIPMODE",
                                              "COMMENT"], index_col=False)

    lineitem_table.set_index(['SUPPKEY'], inplace=True)
    return [nation_table, supplier_table, customer_table, orders_table, lineitem_table]


def semi_join(R1, R2, attr):
    return R1[ R1[attr].isin(R2[attr]) ] # assuming that attr is the index in the previous relation


def W_r0(R, A):
    prod = 1
    n = len(R)
    size = len(R[0])
    #for j in range(1, n): # W(r0) = W(R1), then i=1, j=i+1 to n
    for j in range(1,n):
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

def main_qx(num_samples):
    DEBUG = False
    R = load_tables_qx()
    A = ['NATIONKEY', 'NATIONKEY', 'CUSTKEY', 'ORDERKEY']         # list of join attributes
    samples = []
    num_relations = len(R)
    start = time.time()
    wps = []
    ws = []

    # Compute W(t) beforehands
    for i in range(num_relations):
        if i == 0:
            wp = W_r0(R, A)
        else:
            wp = ws[i-1]
        if i == 0:
            sj = R[0] # r0 joins with all the tuples in R1
        else:
            sj = semi_join(R[i-1], R[i], A[i-1])

        w_sj = W_t(R, i, A)
        w = len(sj) * w_sj
        wps.append(wp)
        ws.append(w)
    print(wps)
    print(ws)

    # Proceed with sampling based on Algorithm 1
    for s in range(num_samples):
        S = []
        for i in range(num_relations-1):
            wp = wps[i]
            w = ws[i]

            prob = 1.0 - w / wp # rejection prob
            #print(prob)

            # reject with the compudated probability
            if random.random() < prob:
                break

            # Sample a tuple t from the semi-joined relations
            t = sj.sample(n=1)
            S.append(t)
        if S != []:
            samples.append(S)

    end = time.time()
    print('Time elapsed: %f seconds' % (end - start))
    print('Collected samples: %d' % len(samples))
    return len(samples), end - start


def main_q3(num_samples):
    DEBUG = False
    R = load_tables_q3()
    A = ['CUSTKEY', 'ORDERKEY']         # list of join attributes
    samples = []
    num_relations = len(R)
    start = time.time()
    wps = []
    ws = []

    # Compute W(t) beforehands
    for i in range(num_relations-1):
        if i == 0:
            wp = W_r0(R, A)
        else:
            #wp = W_t(R, i-1, A)
            wp = ws[i-1]

        if i == 0:
            sj = R[0] # r0 joins with all the tuples in R1
        else:
            sj = semi_join(R[i-1], R[i], A[i-1])
        w_sj = W_t(R, i+1, A)
        w = len(sj) * w_sj

        wps.append(wp)
        ws.append(w)
    print(wps)
    print(ws)

    # Proceed with sampling based on Algorithm 1
    for s in range(num_samples):
        S = []
        for i in range(num_relations-1):
            wp = wps[i]
            w = ws[i]
            prob = 1.0 - w / wp
            #print(prob)

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
    print('Collected samples: %d' % len(samples))
    return len(samples), end - start


if __name__ == '__main__':
    num_samples, _time = main_q3(100)
    print('='*100)
    num_samples, _time = main_qx(100)

