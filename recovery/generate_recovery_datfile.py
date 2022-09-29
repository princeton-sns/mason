import copy
from heapq import heappush, heappop
import numpy as np
import os
import pandas as pd
import re
import sys

dfs = []
if len(sys.argv) >= 2:
    nseq_spaces = int(sys.argv[1])
else:
    nseq_spaces = 1

# column names; append one for each sequence space
names = ["time"]
for n in range(nseq_spaces):
    names.append("seqspace{}".format(n))
print("Column names: {}".format(names))

for file in os.listdir():
    if 'receivedSequenceNumbers' in file:
        print("\tReading in %s..." % file)
        df = pd.read_csv(file, header=None, names=names,
                sep=" ", index_col=False)
        dfs.append(df[names])

dfs = pd.concat([df for df in dfs])

# dfs["time"] = dfs["time"] - dfs["time"].min() # make time start at 0
dfs.dropna(inplace=True)
dfs = dfs.sort_values("time")
for seqspace in names[1:]:
    if not dfs[seqspace].is_unique:
        print(dfs[seqspace].duplicated())
#        df[df.duplicated(keep=False)]
        for i in dfs[seqspace][dfs[seqspace].duplicated(keep=False)]:
            if i != 0:
                # duplicates of 0 are expected, used as NaN for noops
                print("Got non-zero duplicate %d" % i)
                exit()
dfs = dfs.reset_index(drop=True)

# scan dfs and replace values with highest number s.t. all below are filled
#temp = dfs
def f(x):
    if f.count % f.print_interval == 0:
        if f.heap:
            print('iter %d, highest num %d, min %d'%(f.count, f.highest_num, f.heap[0]))
        else:
            print('iter %d, highest num %d, heap is empty'%(f.count, f.highest_num))
        print("x[seq_num]: %d" % x)

    f.count += 1
    if x == 0 and f.highest_num >= 0:
        return f.highest_num
    heappush(f.heap, x)

    if f.do_print:
        print("Highest num: %d, heap top: %d" % (f.highest_num, f.heap[0]))
        print("Does highest_num + 1 == heap[0]? %r" % (f.heap[0] == f.highest_num + 1))
        # print(f.heap)

    try:
        # Check if smallest seqnum up to now matches 1 + the highest seen
        while f.heap[0] == f.highest_num + 1:
            f.highest_num = f.heap[0]
            heappop(f.heap)
    except IndexError:  # Empty heap
        pass

    if f.do_print:
        f.print_interval = 1
    return f.highest_num


for j, seqspace in enumerate(names[1:]):
    print("Working on {}".format(seqspace))
    f.print_interval = 100000
    f.heap = []
    f.highest_num = -1
    f.count = 0
    f.do_print = False

    highest_num = []
    subdf = dfs[['time', seqspace]]
    for idx, time, seqnums in subdf.itertuples():
        highest_num.append(f(seqnums))

    subdf["highest_num"] = highest_num

    grouped = subdf.groupby(pd.cut(subdf["time"], np.arange(
        subdf["time"].min()-1000,
        subdf["time"].max()+1000, 1000))).max()

    grouped = grouped.dropna()

    # makes sure seq_nums are monotonically increasing to print
    # the highest number seen so far
    highest_so_far = 0
    for i in range(0, len(grouped)):
        if grouped.iloc[i][seqspace] < highest_so_far:
            grouped.iloc[i][seqspace] = highest_so_far
        else:
            highest_so_far = grouped.iloc[i][seqspace]

    grouped[["time", "highest_num"]].to_csv("recovery_data_{}.dat".format(j))

    del subdf
    del highest_num
    del grouped
