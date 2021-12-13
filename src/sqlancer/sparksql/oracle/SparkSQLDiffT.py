from filecmp import *
from os import listdir
from os.path import isfile, join

v3_2_0 = 'res/spark-v3.2.0'
v3_0_3 = 'res/spark-v3.0.3'

v3_2_0_files = [f for f in listdir(v3_2_0) if isfile(join(v3_2_0, f))]
v3_0_3_files = [f for f in listdir(v3_0_3) if isfile(join(v3_0_3, f))]

print(v3_2_0_files)
for f in v3_2_0_files:
    if f[-4:] != ".log" or f not in v3_0_3_files:
        continue
    if cmp(join(v3_2_0, f), join(v3_0_3, f), False):
        print('bug detected when comparing', f)