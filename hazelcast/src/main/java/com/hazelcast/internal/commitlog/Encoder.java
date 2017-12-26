package com.hazelcast.internal.commitlog;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import sun.misc.Unsafe;

/**
 * Responsible for encoding and decoding objects from the heap.
 * <p>
 * Instances are only used by a single thread at any given moment, so don't need to be threadsafe.
 * <p>
 * A single DataStream will always have the same DSEncoder type for all entries.
 */
public abstract class Encoder {

    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public InternalSerializationService serializationService;
    // points to the first byte in the region.
    public long dataAddress;
    public int dataOffset;
    public long indicesAddress;
    public int dataLength;

    public abstract HeapData load();

    public abstract boolean store(Object object);
}
