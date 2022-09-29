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

set datafile separator ","
set print "-"

#upgrades seem to have destroyed the ability for gnuplot to produce pdfcairo output for me for now
#set term postscript eps enhanced color linewidth 2 rounded font "Helvetica,14"
#lset size 0.6, 0.45

# from Brighten Godfrey's Blog
# Note you need gnuplot 4.4 for the pdfcairo terminal.
# This allows you to change fonts, among other features.
set term pdfcairo font "Helvetica,24" linewidth 4 rounded enhanced size 5,2.5
#set grid
set border 3 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
set xtics nomirror
set ytics nomirror
set ytics 10

# allows ? to be in the file to show a missing point
set datafile missing "?"

# allows # to be used as comments inside a datafile
set datafile commentschars "#"

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
#was * ALL OF THE LINES OVERLAP
if (!exists("basename")) basename='recovery'
FILES = system("ls -1 recovery_data_*.dat")
LABEL = system("ls -1 recovery_data_*.dat | sed -e 's/recovery_data_//' -e 's/.dat//'")

##############
set xlabel "Time (s)" offset 0,0.5
#set ylabel "Highest Seqnum (M)" offset 1,0
set ylabel "Highest\n Seqnum (M)" offset 0,0

set xtics nomirror
set ytics nomirror
set nokey #key top right
set output basename.".pdf"

set xr[0:30]

#set xr [0:31]
#set xtic 8,2,1024 offset 0,0.4

#set yr [0:]
#set ytic 0,2

warmup = 4
s = 16 + warmup  # sequencer
d = 16 + warmup  # deptracker
l = 6  + warmup  # leader

set arrow from l, graph 0 to l, graph 1 nohead lc rgb "gray"
#set arrow from d, graph 0 to d, graph 1 nohead lc rgb "gray"
set arrow from s, graph 0 to s, graph 1 nohead lc rgb "gray"

plot for [i=1:words(FILES)] word(FILES,i) using ($2/1000000):($3/1000000) with lines title word(LABEL, i) ls 1