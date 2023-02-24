Clients submit operations to proxies and record throughput and latency information.
The output is a file client-#.dat that has "throughput, p50 latency, p99, p999, p9999"

Flags are (flag, default, description):

```
proxy_threadid,     0,  Proxy threadid to connect to
concurrency,        16, Number of concurrent requests per client
nthreads,           16, Number of threads (independent clients) to run on this client
nproxy_leaders,     0,  Number of proxy leaders per proxy machine
nproxy_threads,     0,  Number of proxy threads per proxy machine
expduration,        10, Experiment duration
my_ip,              "", IP address for this machine 
proxy_ip_0,         "", IP address for the proxy n
proxy_ip_1,         "", IP address for the proxy n
proxy_ip_2,         "", IP address for the proxy n
proxy_id,           0,  Proxy id
out_dir,            "", IP address for the proxy n
results_file,       "", File to print results
nsequence_spaces,   0,  Total number of sequence spaces
```

Defaults are different in `../run_experiment.py`, flags should only be modified through `../run_experiment.py`.