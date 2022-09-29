# this file makes it easy to add generating plots to an experiment script
# just replace:
#  XVAR, YVAR
# and then append your plot commands
#  e.g.: "thru_1proc/thru.graph" using 1:9 title "Unmodified" with linespoints

set   autoscale # scale axes automatically
unset log       # remove any log-scaling
unset label     # remove any previous labels
set xtic auto   # set xtics automatically
set ytic auto   # set ytics automatically

set print "-"

#lset size 0.6, 0.45

# from Brighten Godfrey's Blog
# Note you need gnuplot 4.4 for the pdfcairo terminal.
# This allows you to change fonts, among other features.
#### CHANGE THIS FROM png TO pdfcairo FOR DISPLAY!
set term pngcairo font "Helvetica,12" linewidth 2 rounded enhanced
#set grid
set border 3 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
set xtics nomirror
set ytics nomirror

# allows ? to be in the file to show a missing point
set datafile missing "?"

# allows # to be used as comments inside a datafile
set datafile commentschars "#"

set title "Throughput-latency"

#set log x
#set mxtics 10    # Makes logscale look good

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt rgb "#A00000" lw 0.75 pt 1
set style line 2 lt rgb "#00A000" lw 0.75 pt 6
set style line 3 lt rgb "#5060D0" lw 0.75 pt 2
set style line 4 lt rgb "#F25900" lw 0.75 pt 9
set style line 5 lt rgb "#00C5C5" lw 0.75 pt 3
set style line 6 lt rgb "#804000" lw 0.75 pt 4
##############
if (!exists("filename")) filename="datfiles/tputlatency.dat"
##############
set xlabel "Throughput (Mreqs/s)" offset 0,0.5
set ylabel "Latency (us)" offset 1,0
#set ylabel "Throughput (stock\-level/s)" offset 1,0

set xtics nomirror
set ytics nomirror
set key top right
set output "datfiles/tputlatency.png"

#set xr [100000:190000000]
#set xtic 8,2,1024 offset 0,0.4

#set log x
#set log y
#set yr [1:18000]
#set ytic 0,2

plot for [i=0:*] filename index i using 1:2 with linespoints title columnheader(1)
