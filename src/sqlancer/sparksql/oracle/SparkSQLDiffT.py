from filecmp import *
from os import listdir
from os.path import isfile, join

v3_2_0 = 'res/spark-v3.2.0'
v3_0_3 = 'res/spark-v3.0.3'

v3_2_0_files = [f for f in listdir(v3_2_0) if isfile(join(v3_2_0, f))]
v3_0_3_files = [f for f in listdir(v3_0_3) if isfile(join(v3_0_3, f))]

with open("res/out.txt", 'w') as output:
    for f in v3_2_0_files:
        if f[-8:] != "-cur.log":
            continue
        if f not in v3_0_3_files:
            print(f'File {f} not found in both experiment results!')
        if cmp(join(v3_2_0, f), join(v3_0_3, f), False):
            with open(join(v3_2_0, f)) as f1:
                f1_lines = f1.readlines()
            with open(join(v3_0_3, f)) as f2:
                f2_lines = f2.readlines()
            # Find and print the diff:
            for i in range(min(len(f1_lines), len(f2_lines))):
                print(f'bug detected when comparing {f}:\n{f1_lines[i]} != {f2_lines[i]}')
                output.write(f'{f1_lines[i]} != {f2_lines[i]}')