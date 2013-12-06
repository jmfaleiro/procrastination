import sys
import os

    
def do_expt(threshold_list, do_blind, blind_freq):
    cmd = "build/lazy_db --period 1000 --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 --sub_threshold {0} --num_runs 1  --experiment 0 "
    for threshold in threshold_list:
        output_string = cmd.format(str(threshold))
        if do_blind:
            output_string += "--blind_writes " + str(blind_freq)
        os.system(output_string)
        

def main():
#    for i in range(0, 5):
#        do_expt([50,100,150,200,250,300], False, -1)

    for j in [2, 4, 8, 16, 32]:
        for i in range(0, 10):
            do_expt([300], True, j)

    for i in range(0, 10):
        os.system("build/lazy_db --num_workers 1 --num_reads 0 --num_writes 20 --num_records 1000000 --num_txns 10000000 ---sub_threshold 300 --num_runs 1 --experiment 0")

if __name__ == "__main__":
    main()
