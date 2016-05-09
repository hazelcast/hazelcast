package com.hazelcast.internal.memory.impl;

import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class UnsafeUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(UnsafeUtil.class);
    }

    @Test
    public void testUnsafe() throws Exception {
        if (UnsafeUtil.UNSAFE_AVAILABLE) {
            assertNotNull(UnsafeUtil.UNSAFE);
        } else {
            assertNull(UnsafeUtil.UNSAFE);
        }
    }
}
