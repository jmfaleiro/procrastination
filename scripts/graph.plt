set autoscale
set terminal png
unset log
unset label
set xtic auto
set ytic auto
set title "CDF plot"
set xlabel "Latency (cycles)"
set ylabel "Fraction"
set logscale x
set xr [1:100000000]
set yr [0:1.2]
plot "eager.txt" using 2:1 title "eager" with lines, \
     "latency_lazy_50.txt" using 2:1 title "lazy 50" with lines, \
     "latency_lazy_100.txt" using 2:1 title "lazy 100" with lines, \
     "latency_lazy_150.txt" using 2:1 title "lazy 150" with lines, \
	 "latency_lazy_200.txt" using 2:1 title "lazy 200" with lines, \
	 "latency_lazy_250.txt" using 2:1 title "lazy 250" with lines, \
	 "latency_lazy_300.txt" using 2:1 title "lazy 300" with lines

