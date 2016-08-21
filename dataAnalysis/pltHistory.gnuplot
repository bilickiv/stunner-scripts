 set title "Data usage over the last 24 hours"
 unset multiplot
 set xdata time
 set boxwidth 0.5
 set style fill solid 
 set terminal xterm
 set timefmt '%Y-%m-%d %H:%M:%S'
 set format x "%Y-%m-%d %H:%M"
 set xlabel "Time"
 set ylabel "Traffic"
 set datafile separator ";" 
 set autoscale y  
 set xrange ['2011-01-01 01:08:09':'2014-01-03 20:09:55']
 plot "/Users/bilickiv/developer/stunner-scripts/results/userlog/clZrY1JIZ01OK3d5MHR0a1dpdm9XK1BQeUY1b0d0OEZOVXRvb0tsT1hMbz0K.imp" using 3:5:xtic(2) with boxes 
