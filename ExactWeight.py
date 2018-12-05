import pandas
import numpy
table = pandas.read_table('data_self_generated/customer.tbl', delimiter="|", header=None)
table = table.iloc[:, :-1]
pass