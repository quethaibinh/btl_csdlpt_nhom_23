import os
import tempfile
import shutil

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
RATINGS_FILE = os.path.join(CURR_DIR, "./ml-10m/ml-10M100K/ratings.dat")
OUTPUT_FILE = os.path.join(CURR_DIR, "./Optimize/test_data.dat")

NUMBER_OF_LINES = 10000
lines = []
with open(RATINGS_FILE, "r") as f:
    count = 0
    for line in f:
        lines.append(line)
        count += 1
        if count >= NUMBER_OF_LINES:
            break

with open(OUTPUT_FILE, "w") as f:
    for line in lines:
        print(line)
        f.write(line)

print(f"Length of data: {len(lines)}")
print(f"Data refined and saved to {OUTPUT_FILE}")