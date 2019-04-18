
This document covers how to setup medusa from git and restore a cluster.

Install medusa for restore on the new restore machines for example from basesusers.

```
medusa-restore-setup medusa-backups readaheadcass-medusa-backup readaheadcassrestore gew1
```

Install debian packages
```
sudo apt-get install -y debhelper python3 dh-virtualenv libffi-dev libssl-dev python3-dev libxml2-dev libxslt-dev build-essential python3-pip
```

There is a problem with setuptools. The required package has lower apt priority. Needs manual intervention.
```
# manually, aptitude, say no until it proposes installing the right version
sudo aptitude install -y python3-setuptools=20.7.0-1

# Alternatve: we had good result with just running this instead of the previous command.
sudo pip3 install setuptools --upgrade
```

Install python packages. Will take time to build java-driver.
```
sudo pip3 install Click==6.7 PyYAML==3.10 google-cloud-storage==1.7.0 cassandra-driver==3.14.0 paramiko==2.4.1 psutil==5.4.7 ffwd==0.0.2
```

Install medusa from github. Will take time to build java-driver again.
````
sudo pip3 install git+https://radovanz@ghe.spotify.net/data-bye/medusa.git@radovanz/CASS-68 --upgrade
```

Install medusa for restore on the new restore machines for example from basesusers.

Get the restore mapping, for example from basesusers:
```
hecuba2-cli show-restore-mapping --source-role readaheadcass --target-role readaheadcassrestore
```

Should look like:
```
# format: token, seed, target, source
-9223372036854775808,True,gew1-readaheadcassrestore-a-2whw.gew1.spotify.net,gew1-readaheadcass-a-hg68.gew1.spotify.net
-3074457345618258603,True,gew1-readaheadcassrestore-a-snlr.gew1.spotify.net,gew1-readaheadcass-a-nmdp.gew1.spotify.net
3074457345618258602,False,gew1-readaheadcassrestore-a-035r.gew1.spotify.net,gew1-readaheadcass-a-r9n0.gew1.spotify.net
```

Run the restore test:
```
medusa -v restore-cluster --backup-name 2018121213 --host-list mapping.txt
```

Running restore in-place
```
medusa -v restore-cluster --backup-name 2018121213 --seed-target gew1-readaheadcass-a-nmdp.gew1.spotify.net
```
