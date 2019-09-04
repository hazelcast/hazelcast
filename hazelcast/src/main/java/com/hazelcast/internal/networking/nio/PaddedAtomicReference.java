package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

class PaddedAtomicReferenceSuper {
    private Object p0;
    private Object p1;
    private Object p2;
    private Object p3;
    private Object p4;
    private Object p5;
    private Object p6;
    private Object p7;
    private Object p8;
    private Object p9;
    private Object pa;
    private Object pb;
    private Object pc;
    private Object pd;
    private Object pe;
    private Object pf;
}

class PaddedAtomicReferenceFields<V> extends PaddedAtomicReferenceSuper {
    protected volatile V value;
}

class PaddedAtomicReferenceSub<V> extends PaddedAtomicReferenceFields<V> {
    private Object p0;
    private Object p1;
    private Object p2;
    private Object p3;
    private Object p4;
    private Object p5;
    private Object p6;
    private Object p7;
    private Object p8;
    private Object p9;
    private Object pa;
    private Object pb;
    private Object pc;
    private Object pd;
    private Object pe;
    private Object pf;
}

public class PaddedAtomicReference<V> extends PaddedAtomicReferenceSub<V> {
    private static final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private static final long valueOffset;

    static {
        try {
            valueOffset = unsafe.objectFieldOffset(PaddedAtomicReferenceFields.class.getDeclaredField("value"));
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    public PaddedAtomicReference(){
        this(null);
    }


    public PaddedAtomicReference(V value){
        set(value);
    }
    
    /**
     * Gets the current value.
     *
     * @return the current value
     */
    public final V get() {
        return value;
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     */
    public final void set(V newValue) {
        value = newValue;
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect the expected value
     * @param update the new value
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public final boolean compareAndSet(V expect, V update) {
        return unsafe.compareAndSwapObject(this, valueOffset, expect, update);
    }

}
