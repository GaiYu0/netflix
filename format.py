import sys
from tqdm import tqdm

txt = open(sys.argv[1])
csv = open(sys.argv[2], 'w')
for line in tqdm(txt):
    if ':' in line:
        mid = line.strip().replace(':', ',')
    else:
        csv.write(mid + line)
