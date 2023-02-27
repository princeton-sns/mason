appends='unset'
if [[ $* == *--appends* ]]; then
    appends=true
elif [[ $* == *--reads* ]]; then
    appends=false
fi
if [[ $appends = 'unset' ]]; then
    echo Please choose --appends or --reads.
    exit
fi

ntrials=1
CUR_DIR=`pwd`
for i in  1 2 3 4; do
    nproxies=$(($i*6))
    nproxymachines=$(($nproxies*3))
    nclients=$nproxies # corfumason
    # nclients=$(($i*4)) # corfu

    # echo $nproxies $nproxymachines $nclients
    for conc in  1 2 4 8 16 32 64; do
        for j in `seq 1 $ntrials`; do
            echo Beginning run $j with $nclients clients conc $conc and $i shards
            sleep 3
            while :
            do
                timestamp=`date +%s`
                dir=results-$timestamp
                # corfumason
                if [[ $appends = true ]]; then
                    python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc --nproxies $nproxymachines \
                       --nproxy_threads 8 --nproxy_leaders $(($nproxies*8)) --ncorfu_servers $((i*2)) \
                       --nclients $nclients
                else
                    python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc --nproxies $nproxymachines \
                       --nproxy_threads 8 --nproxy_leaders $(($nproxies*8)) --ncorfu_servers $((i*2)) \
                       --nclients $nclients --max_log_position 524288
                fi

                # corfu
                # if [[ $appends = true ]]; then
                #     python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc \
                #       --ncorfu_servers $((i*2)) --nclients $nclients
                # else
                #     python3 run_experiment.py $1 --outdir $dir --client_concurrency $conc \
                #       --ncorfu_servers $((i*2)) --nclients $nclients --max_log_position 524288
                # fi
                bash parse_datfiles.sh results
                cd results/$dir
                # nclients=$(expr $(ls proxy* | wc -l) / 3)
                # nclients=$(expr $(ls proxy* | wc -l) / 3)
                echo $nclients
                if [ $nclients -ne `ls client-*.dat | wc -l` ]
                then
                    cd $CUR_DIR
                    echo Suspected client failure force removing results/$dir in 3 seconds.
                    sleep 3
                    rm -fr results/$dir
                    continue
                fi
                cd $CUR_DIR
                echo "Done run $j of concurrency $conc and $i shards"
                sleep 3
                break
            done
        done
    done
done
