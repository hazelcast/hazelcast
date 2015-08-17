/*
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

/*
 *
 *
 *
 *
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package com.hazelcast.util;

import com.hazelcast.nio.UnsafeHelper;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicReference;

/**
 * An object reference that may be updated atomically. See the {@link
 * java.util.concurrent.atomic} package specification for description
 * of the properties of atomic variables.
 *
 * @param <V> The type of object referred to by this reference
 * @author Doug Lea
 * @since 1.5
 */
public class PaddedAtomicReference<V> {

    private static final Unsafe unsafe = UnsafeHelper.UNSAFE;
    private static final long valueOffset;

    static {
        try {
            valueOffset = unsafe.objectFieldOffset
                    (AtomicReference.class.getDeclaredField("v8"));
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    private volatile V v1;
    private volatile V v2;
    private volatile V v3;
    private volatile V v4;
    private volatile V v5;
    private volatile V v6;
    private volatile V v7;
    private volatile V v8;
    private volatile V v9;
    private volatile V v10;
    private volatile V v11;
    private volatile V v12;
    private volatile V v13;
    private volatile V v14;
    private volatile V v15;
    private volatile V v16;

    public PaddedAtomicReference(V initialValue) {
        v8 = initialValue;
    }

    public PaddedAtomicReference() {
    }

    public final V get() {
        return v8;
    }

    /**
     * Sets to the given value.
     *
     * @param newValue the new value
     */
    public final void set(V newValue) {
        v8 = newValue;
    }


    public final boolean compareAndSet(V expect, V update) {
        return unsafe.compareAndSwapObject(this, valueOffset, expect, update);
    }

}
