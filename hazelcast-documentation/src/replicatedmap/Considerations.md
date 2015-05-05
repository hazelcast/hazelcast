

### For Consideration

A replicated map **does not** support ordered writes! In case of a conflict caused by two nodes simultaneously written to the
same key, a vector clock algorithm resolves and decides on one of the values.

Due to the weakly consistent nature and the previously mentioned behaviors of replicated map, there is a
chance of reading stale data at any time. There is no read guarantee like there is for repeatable reads.
