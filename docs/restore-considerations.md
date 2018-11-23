
* Restoring a node without `--in-place`, meaning restoring a node to a different HW works only if the node has never been in the cluster (or is a seed). The reason for this is that we _must_ purge `system.local` because we want the node to own the tokens of the data it is restoring.

* Restoring a cluster is potentially flaky because `restore-cluster` starts all nodes at once. If the seeds take too long to start, non-seeds will give up waiting for them.
