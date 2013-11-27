import sys
import os

def main():
    for i in range(0, 5):
        os.system("build/lazy_db --period 1 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 300 --num_runs 1 --normal 40")


if __name__ == "__main__":
    main()
