import sys
import os


def do_client_latency(outdir, blind_freq, threshold):
    cmd = "build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20  --num_records 1000000 --num_txns 10000000 --sub_threshold {0} --num_runs 1  --experiment 1 --blind_writes {1}"
    output_string = cmd.format(str(threshold), str(blind_freq))
    os.system(output_string)
    out_fmt = "threshold_{0}_freq_{1}.txt"
    out_file = out_fmt.format(str(threshold), str(blind_freq))
    out_file_path = os.path.join(outdir, out_file)
    os.system("mv client_latencies.txt " + out_file_path)

def main():
    if not os.path.isdir(sys.argv[1]):
        os.mkdir(sys.argv[1])

    blind_frequencies = [2, 4, 8, 16, 32]
    for freq in blind_frequencies:
        do_client_latency(sys.argv[1], freq, 300)

if __name__ == "__main__":
    main()
