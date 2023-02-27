setDatas='unset'
if [[ $* == *--setDatas* ]]; then
    setDatas=true
elif [[ $* == *--getDatas* ]]; then
    setDatas=false
fi
if [[ $setDatas = 'unset' ]]; then
    echo Please choose --setDatas or --getDatas.
    exit
fi

CUR_DIR=`pwd`
for i in 1; do
    nproxies=1
    nproxymachines=$(($nproxies*3))
    nclients=$nproxies
    for conc in 1 2 4 8 16; do
        for max_log_size in 500000; do # unused
            for j in `seq 1 `; do
                echo Beginning run $j with $nclients clients conc $conc and $i shards
                sleep 3
                while :
                do
                    timestamp=`date +%s`
                    dir=results-$timestamp
                    if [[ $setDatas = true ]]; then
                        python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc --nproxies $nproxymachines \
                           --nsequence_spaces $i --nclients $nclients --write_percent 1 --read_percent 0
                    else
                        python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc --nproxies $nproxymachines \
                           --nsequence_spaces $i --nclients $nclients --write_percent 0 --read_percent 1
                    fi
                    bash parse_datfiles.sh results | tee last-results
                    cd results/$dir
                    grep BUS proxy-*
                    # grep dequeueing proxy-* | head -1
                    # grep dequeueing proxy-* | tail -1
                    echo $nclients
                    if [ $nclients -ne `ls client-*.dat | wc -l` ]
                    then
                        cd $CUR_DIR
                        echo A client failed force removing results/$dir in 3 seconds.
                        sleep 3
                        rm -fr results/$dir
                        continue
                    fi
                    #grep 'BUS\|Total\|raw' proxy-*
                    cd $CUR_DIR
                    echo "Done run $j of concurrency $conc and $i shards max_log_size $max_log_size"
                    sleep 3
                    break
                done
            done
        done
    done
done
