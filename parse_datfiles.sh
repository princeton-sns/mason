tmp=0
for dir in $1/results-*; do
    fname="clients.concat"
    cd $dir

    counter=0
    for i in $(find . -name "*.dat"); do
	#       echo $i
	cat $i >> $fname
	echo >> $fname
	counter=$(($counter+1))
    done

    # throughput
    tput=`awk -F',' '{sum+=$1} END {printf "%f", sum}' $fname`

    # latency
    lat=`awk -v counter="$counter" -F',' '{sum+=$2} END {printf "%.3f\n", sum/counter}' $fname`

    echo $tput $lat

    tmp=`expr $tmp + 1`
    if (( $tmp % 5 == 0 ))
    then
	echo ''
    fi

    rm $fname

    cd ../..

done
