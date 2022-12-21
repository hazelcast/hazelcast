## Summary

This document describes how tpc should be configured.

## Description

All tpc related config should be in a separate node called `tpc` because
having all related configuration in one place is cleaner.

Regarding port configuration user will only be able to configure an
integer. Server will create `eventloop-count` ports starting from that
integer. For example, if `port` is 12000 and `eventloop-count` is 10,
ports [12000-12009] will be used.

You can see a part of the `tpc` configuration below:
```
hazelcast/
└── tpc/
    ├── enabled - boolean
    ├── engine/
    │   ├── eventloop-count - integer
    │   └── nio-eventloop/
    │       ├── localRunQueueCapacity - integer
    │       └── concurrentRunQueueCapacity - integer
    └── server/
        ├── port - integer
        ├── receiveBufferSize - integer
        └── sendBufferSize - integer
```
