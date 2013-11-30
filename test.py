import sys
import os

def do_expt(direct_string):
    prefix_string = "scripts/" + direct_string + "/"
    print prefix_string

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 300 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_300.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 250 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_250.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 200 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_200.txt")


    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 150 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_150.txt")
    
    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 100 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_100.txt")
    
    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 50 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "lazy_50.txt")
    
    os.system("build/lazy_db --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 250 --num_runs 1 --normal 30 --experiment 0")
    os.system("mv latencies.txt " + prefix_string + "eager.txt")    

def main():
    do_expt("small")



if __name__ == "__main__":
    main()
