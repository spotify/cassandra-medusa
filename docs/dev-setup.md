# Developer Setup

This document describes how to setup medusa straight from Github.

First, Install Debian packages:
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

Then, install the python packages. Will take time to build java-driver.
```
sudo pip3 install Click==6.7 PyYAML==3.10 google-cloud-storage==1.7.0 cassandra-driver==3.14.0 paramiko==2.4.1 psutil==5.4.7 ffwd==0.0.2
```

Finally, install Medusa from source. Will take time to build java-driver again.
````
sudo pip3 install git+https://you@github.com/spotify/medusa.git@branch --upgrade
```
