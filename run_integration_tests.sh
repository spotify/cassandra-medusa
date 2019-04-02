#!/usr/bin/env bash
export LOCAL_JMX=yes
export PYTHONWARNINGS="ignore"
pip3 install -r requirements.txt
pip3 install -r requirements-test.txt
cd tests/integration
PYTHONPATH=../.. aloe features/integration_tests.feature --verbosity=2 -v
