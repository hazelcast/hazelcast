package com.hazelcast.internal.commitlog;

import com.hazelcast.internal.serialization.impl.HeapData;

public class Load {
    // this is inputparameter; so make sure it is set.
    public long offset;
    // this is output parameter
    public HeapData value;
    // this is output parameter
    // number of bytes read for the load.
    // this can be used for the next load.
    public int length;
}
