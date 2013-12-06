import sys
import os


def do_client_latency(outdir, threshold):
    cmd = "build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20  --num_records 1000000 --num_txns 10000000 --sub_threshold {0} --num_runs 1 --normal 30  --experiment 1"
    output_string = cmd.format(str(threshold))
    os.system(output_string)
    out_fmt = "lazy_threshold_{0}.txt"
    out_file = out_fmt.format(str(threshold))
    out_file_path = os.path.join(outdir, out_file)
    os.system("mv client_latencies.txt " + out_file_path)

def main():
    if not os.path.isdir(sys.argv[1]):
        os.mkdir(sys.argv[1])

    thresholds = [50, 100, 150, 200, 250, 300]
    for thresh in thresholds:
        do_client_latency(sys.argv[1], thresh)

if __name__ == "__main__":
    main()
