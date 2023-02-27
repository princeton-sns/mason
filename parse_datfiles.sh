# Given a dir in $1, for each subdirectory, this script parses all client-*.dat 
# files aggregates the throughput and averages the latencies outputting:
#   aggregate-throughput-(Mops/s) avg 50% 99% 99.9% 99.99% latencies
# e.g. run 'bash parse_datfiles.sh results' to parse all dirs in results/
tmp=0
for dir in $1/results-*; do
    fname="clients.concat"
    cd $dir
#    echo $dir

    counter=0
    for i in $(find . -name "*.dat"); do
	    cat $i >> $fname
    	echo >> $fname
	    counter=$(($counter+1))
    done

    # throughput
    tput=`awk -F',' '{sum+=$1} END {printf "%f", sum}' $fname`

    # latency
    lat=`awk -v counter="$counter" -F',' '{sum+=$2} END {printf "%.3f\n", sum/counter}' $fname`
    lat99=`awk -v counter="$counter" -F',' '{sum+=$3} END {printf "%.3f\n", sum/counter}' $fname`
    lat999=`awk -v counter="$counter" -F',' '{sum+=$4} END {printf "%.3f\n", sum/counter}' $fname`
    lat9999=`awk -v counter="$counter" -F',' '{sum+=$5} END {printf "%.3f\n", sum/counter}' $fname`

    echo $tput $lat $lat99 $lat999 $lat9999 

    tmp=`expr $tmp + 1`
    # we were running 5 trials and so put a blank line in between them, but for 1 trial it doesn't make sense.
    # if (( $tmp % 5 == 0 ))
    # then
	#     # echo ''
    # fi

    rm $fname

    cd ../..
done
