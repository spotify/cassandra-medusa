#!/usr/bin/env bash
set -x
# Keep the following rm for the sake of running the integration tests in CI
rm -Rf .python-version
export LOCAL_JMX=yes
export PYTHONWARNINGS="ignore"
pip3 install -r requirements.txt
pip3 install -r requirements-test.txt
cd tests/integration
if [ -z "$1" ]
then
	PYTHONPATH=../.. aloe features/integration_tests.feature --verbosity=2 -v
else
	PYTHONPATH=../.. aloe features/integration_tests.feature --verbosity=2 -v -n $1
fi

