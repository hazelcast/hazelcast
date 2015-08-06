package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NullGetterTest {

    @Test
    public void test_getValue() throws Exception {
        Object value = NullGetter.NULL_GETTER.getValue(new Object());

        assertNull(value);
    }

    @Test
    public void test_getReturnType() throws Exception {
        Class returnType = NullGetter.NULL_GETTER.getReturnType();

        assertNull(returnType);
    }

    @Test
    public void test_isCacheable() throws Exception {
        boolean cacheable = NullGetter.NULL_GETTER.isCacheable();

        assertFalse(cacheable);
    }
}