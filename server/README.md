The ZK-Mason server. Waits for sequenced operations and processes them in order.

Flags are (flag, default, description):
```
my_ip,              "", This Corfu Server's IP address for eRPC setup
client_ips,         "", All of the clients IPs for watches
nclient_threads,    0,  number of client threads per machine
proxy_ip_0,         "", IP address for the proxy n
proxy_ip_1,         "", IP address for the proxy n
proxy_ip_2,         "", IP address for the proxy n
proxy_id,           0,  Start of the proxy ids
nzk_servers,        0,  Number of CATSKeeper servers.
```

Defaults are different `../run_experiment.py`; flags should only be modified through `../run_experiment.py`.