Corfu servers are chain replicated driven by clients in the `corfu` branch and driven by proxies in the `corfumason` branch.

Flags are (flag, default, description):
```
my_ip,              "", This Corfu Server's IP address for eRPC setup
ncorfu_servers,     0,  The number of servers in the Corfu array
max_log_position,   0,  What to fill up to if read only experiment
```

Defaults are different in `../run_experiment.py` and flags should only be modified through `../run_experiment.py`.