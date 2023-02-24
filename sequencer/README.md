The multi-sequencer. It is unreplicated. If am_backup is set it will sit idle until receiving a recovery message from a proxy.

Flags are (flag, default, description):
```
am_backup,          false,  Set to true if this sequencer is a backup
my_ip,              "",     Sequencer IP address for eRPC setup
other_ips,          "",     IP addresses to connect to if this is the backup
out_dir,            "",     Output directory
nleaders,           0,      Total number of proxy groups (needed for recovery)
nsequence_spaces,   0,      Total number of sequence spaces.
```

Defaults are different in `../run_experiment.py`; flags should only be modified in `../run_experiment.py`.