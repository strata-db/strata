Integration tests that exercise multi-node, recovery, failover, and long-running
cluster behavior through the public `t4` API live here.

The root package keeps faster unit-style and white-box tests close to the core
implementation, while this directory holds black-box scenarios that are slower
and more operational in nature.
