package com.hazelcast.internal.metrics.impl;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COLLECTION;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_COUNTER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_DOUBLE_PRIMITIVE;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_LONG_NUMBER;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_MAP;
import static com.hazelcast.internal.metrics.impl.ProbeUtils.TYPE_PRIMITIVE_LONG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ProbeUtilsTest extends HazelcastTestSupport {

    @Test
    public void testPrivateConstructor() {
        assertUtilityConstructor(ProbeUtils.class);
    }

    @Test
    public void isDouble() {
        assertTrue(ProbeUtils.isDouble(TYPE_DOUBLE_NUMBER));
        assertTrue(ProbeUtils.isDouble(TYPE_DOUBLE_PRIMITIVE));

        assertFalse(ProbeUtils.isDouble(TYPE_PRIMITIVE_LONG));
        assertFalse(ProbeUtils.isDouble(TYPE_LONG_NUMBER));
        assertFalse(ProbeUtils.isDouble(TYPE_COLLECTION));
        assertFalse(ProbeUtils.isDouble(TYPE_MAP));
        assertFalse(ProbeUtils.isDouble(TYPE_COUNTER));
    }
}
