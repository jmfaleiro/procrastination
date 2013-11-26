import sys
import os

def main():
    
    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 300 --num_runs 1 --output_file output.txt  --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/latency_lazy_300.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 250 --num_runs 1 --output_file output.txt --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/latency_lazy_250.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 200 --num_runs 1 --output_file output.txt  --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/latency_lazy_200.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 150 --num_runs 1 --output_file output.txt  --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/latency_lazy_150.txt")

    os.system("build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 100 --num_runs 1 --output_file output.txt  --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/latency_lazy_100.txt")

    os.system("build/lazy_db --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold 100 --num_runs 1 --output_file output.txt  --latency")
    os.system("mv client_latencies.txt scripts/client_latency/uniform/eager.txt")
    
    os.system("cd scripts/uniform/client_latency")
    os.system("gnuplot graph.plt > client.png")

if __name__ == "__main__":
    main()
