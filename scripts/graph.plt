set autoscale
set terminal png
unset log
unset label
set xtic auto
set ytic auto
set title "CDF plot"
set xlabel "Latency (cycles)"
set ylabel "Fraction"
set xr [0:5000]
set yr [0:1.2]
plot "eager.txt" using 2:1 title "eager" with lines, \
     "lazy_100.txt" using 2:1 title "lazy 100" with lines, \
     "lazy_1000.txt" using 2:1 title "lazy 1000" with lines, \
     "lazy.txt" using 2:1 title "lazy" with lines
