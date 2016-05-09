package com.hazelcast.internal.memory.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
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
