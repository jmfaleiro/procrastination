import numpy
import sys
import os
import matplotlib.pyplot as plt

lazy_throughput_fmt = 'lazy_1000_subst_threshold_{0}_normal_30.txt'
txn_latency_fmt = 'lazy_{0}_txn_latency.txt'
system_latency_fmt = 'lazy_{0}_system_latency.txt'



# Cache locality based experiments
def do_cache_locality(threshold_list, folder):
    cmd = 'build/lazy_db {0} --num_workers 1 --num_reads 0 --num_writes 20 ' \
          '--num_records 1000000 --num_txns 10000000 --sub_threshold {1} ' \
          '--normal 30 --num_runs 1 --experiment {2} {3}'
    
    # Define a bunch of useful format strings to use. 
    lazy_throughput_fmt = 'lazy_1000_subst_threshold_{0}_normal_30.txt'
    txn_latency_fmt = 'lazy_{0}_txn_latency.txt'
    system_latency_fmt = 'lazy_{0}_sys_latency.txt'

    for threshold in threshold_list:
        cmd_string = cmd.format('--period 1000', str(threshold), '0', '')
#        for i in range(0, 10):
        os.system(cmd_string)

#        throughput_file = lazy_throughput_fmt.format(str(threshold))
        txn_latency_file = txn_latency_fmt.format(str(threshold))
        system_latency_file = system_latency_fmt.format(str(threshold))

#        os.system('mv ' + throughput_file + ' ' + folder)
        txn_latency_destination = os.path.join(folder, txn_latency_file)
        sys_latency_destination = os.path.join(folder, system_latency_file)
        os.system('mv txn_latencies.txt ' + txn_latency_destination)
        os.system('mv system_latencies.txt ' + sys_latency_destination)


    eager_string = cmd.format('', '300', '0', '')
    for i in range(0, 10):
        os.system(eager_string)

#    os.system('mv eager_normal_30.txt ' + folder)
    txn_latency_destination = os.path.join(folder, 'eager_txn_latency.txt')
    sys_latency_destination = os.path.join(folder, 'eager_sys_latency.txt')
    os.system('mv txn_latencies.txt ' + txn_latency_destination)
    os.system('mv system_latencies.txt ' + sys_latency_destination)
    
def do_txn_cdf(threshold_list, fmt_string, eager_file, plt_name, div_factor):
    
    fig, ax = plt.subplots(1,1)
    legend_fmt = 'lazy_{0}'
    for thresh in threshold_list:
        cur_file = fmt_string.format(str(thresh))
        input_file = open(cur_file, 'r')
        x_list = []
        y_list = []
        for line in input_file:             
            parts = line.split()
            y_list.append(float(parts[0]))
            x_list.append((1.0*long(parts[1])) / div_factor)
        ax.plot(x_list, y_list, label=legend_fmt.format(str(thresh)))


    eager_input = open(eager_file, 'r')
    y_list = []
    x_list = []
    for line in eager_input:
        parts = line.split()
        y_list.append(float(parts[0]))
        x_list.append((1.0*long(parts[1])) / div_factor)
    ax.plot(x_list, y_list, label='eager')
    return [fig, ax]
    
def do_throughput_average(file_name):
    input_file = open(file_name, 'r')
    total_time = 0.0
    num_done = 0
    
    throughput_list = []

    for line in input_file:
        parts = line.split()
        cur_time = float(parts[0])
        num_done = long(parts[1])
        
        cur_thpt = (1.0*num_done) / cur_time
        throughput_list.append(cur_thpt)
    ret = {}
    ret['avg'] = sum(throughput_list)/len(throughput_list)
    ret['min'] = min(throughput_list)
    ret['max'] = max(throughput_list)

    input_file.close()
    return ret


def do_cache_plots(threshold_list, folder):

    lazy_throughput_fmt = 'lazy_1000_subst_threshold_{0}_normal_30.txt'
    y_max = []
    y_min = []
    x_axis = []
    y_axis = []
    for threshold in threshold_list:
        cur_file = lazy_throughput_fmt.format(str(threshold))
        x_axis.append(threshold)
        throughput_dict = do_throughput_average(cur_file)
        y_axis.append(throughput_dict['avg'])
        y_max.append(throughput_dict['max'])
        y_min.append(throughput_dict['min'])

    
    eager_dict = do_throughput_average('eager_normal_30.txt')
    y_eager = []
    min_eager = []
    max_eager = []
    for threshold in threshold_list:        
        y_eager.append(eager_dict['avg'])
        min_eager.append(eager_dict['min'])
        max_eager.append(eager_dict['max'])

    fig,  ax = plt.subplots(1,1)
    
    ax.set_autoscaley_on(False)
    ax.set_ylim([250000,650000])
    ax.set_xlim([50,250])

    ax.plot(x_axis, y_axis, 'b--o', label='lazy')
    ax.plot(x_axis, y_eager, 'r-.o', label='eager')
    ax.legend()
    fig.savefig('cache_throughput.pdf')

def main():
#    do_cache_locality([50,100,150,200,250], 'final')
    os.chdir('final')
    
    do_cache_plots([50,100,150,200,250], 'final')
    
    fig1 = do_txn_cdf([50, 100, 150, 200, 250],
                      'lazy_{0}_txn_latency.txt',
                      'eager_txn_latency.txt',
                      'txn_latency_cdf.pdf',
                      1.0)
    ax1 = fig1[1]
    ax1.set_xscale('log')
    ax1.set_autoscaley_on(False)
    ax1.set_ylim([0,1.2])
    ax1.set_xlim([100,10000])

    ax1.set_title('CDF Single Txn Execution Time')
    ax1.set_xlabel('Time in Cycles')
    ax1.set_ylabel('Fraction')
    ax1.legend(loc='lower right')

    fig1[0].savefig('txn_latency.pdf')


    fig2 = do_txn_cdf([50, 100, 150, 200, 250],
                      'lazy_{0}_sys_latency.txt',
                      'eager_sys_latency.txt',
                      'client_latency_cdf.pdf',
                      1996.0)
    
    ax2 = fig2[1]
    ax2.set_title('CDF Client Observed Latency')
    ax2.set_xlabel('Time in Microseconds')
    ax2.set_ylabel('Fraction')

    ax2.set_xscale('log')
    ax2.set_autoscaley_on(False)
    ax2.set_ylim([0,1.2])
    ax2.set_xlim([0.1,10000])
    ax2.legend(loc='lower right')

    fig2[0].savefig('client_latency.pdf')
               

if __name__ == "__main__":
    main()
