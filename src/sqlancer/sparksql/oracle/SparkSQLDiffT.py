from filecmp import *
from os import listdir
from os.path import isfile, join

v3_2_0 = 'res/spark-v3.2.0'
v3_0_3 = 'res/spark-v3.0.3'

v3_2_0_files = [f for f in listdir(v3_2_0) if isfile(join(v3_2_0, f))]
v3_0_3_files = [f for f in listdir(v3_0_3) if isfile(join(v3_0_3, f))]

for f in v3_2_0_files:
    if f[-8:] != "-cur.log":
        continue
    if f not in v3_0_3_files:
        print(f'File {f} not found in both experiment results!')
    if cmp(join(v3_2_0, f), join(v3_0_3, f), False):
        import difflib
        with open(join(v3_2_0, f)) as f1:
            f1_text = f1.read()
        with open(join(v3_0_3, f)) as f2:
            f2_text = f2.read()
        # Find and print the diff:
        for line in difflib.unified_diff(f1_text, f2_text, join(v3_2_0, f), tofile=join(v3_0_3, f), lineterm=''):
            print(f'bug detected when comparing {f}:\n{line}')