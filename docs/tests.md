
Here are the tests scenarios we should check before we trust in the `restore-cluster` command.


For `restore-cluster`:

in place:
- target nodes ALL up - OK!
- at least 1 target node down - no go, failure when creating token mapping


restore test:
- target nodes up - OK!
- target nodes down - spcassandra-stop when C* is stopped has RC != 0, which breaks restore-cluster


For `restore-node`

in place:
- no nodes up - OK!
- no seeds up - OK!

restore test
- this only works if the node has never been in the cluster