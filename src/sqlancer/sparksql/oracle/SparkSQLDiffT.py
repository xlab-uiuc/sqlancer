from filecmp import *
from os import listdir
from os.path import isfile, join

v1 = 'res/spark0'
v2 = 'res/spark1'

v1_files = [f for f in listdir(v1) if isfile(join(v1, f))]
v2_files = [f for f in listdir(v2) if isfile(join(v2, f))]

with open("res/out.txt", 'w') as output:
    for f in v1_files:
        if f[-8:] != "-cur.log":
            continue
        if f not in v2_files:
            print(f'File {f} not found in both experiment results!')
        if cmp(join(v1, f), join(v2, f), False):
            with open(join(v1, f)) as f1:
                f1_lines = f1.readlines()
            with open(join(v2, f)) as f2:
                f2_lines = f2.readlines()
            # Find and print the diff:
            for i in range(min(len(f1_lines), len(f2_lines))):
                print(f'bug detected when comparing {f}:\n{f1_lines[i]} != {f2_lines[i]}')
                output.write(f'{f1_lines[i]} != {f2_lines[i]}')
