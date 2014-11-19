

### For Consideration

A replicated map **does not** support ordered writes! In case of a conflict caused by two nodes simultaneously written to the
same key, a vector clock algorithm is used to resolve and decide on one of the values.

Due to the weakly consistent nature and the previously mentioned behaviors of replicated map, there is a
chance of reading stale data at any time. There is no read guarantee like repeatable reads.
