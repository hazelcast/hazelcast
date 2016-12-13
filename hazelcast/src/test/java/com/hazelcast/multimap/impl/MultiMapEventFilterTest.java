package com.hazelcast.multimap.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapEventFilterTest {

    private MultiMapEventFilter multiMapEventFilter;
    private MultiMapEventFilter multiMapEventFilterSameAttributes;

    private MultiMapEventFilter multiMapEventFilterOtherIncludeValue;
    private MultiMapEventFilter multiMapEventFilterOtherKey;
    private MultiMapEventFilter multiMapEventFilterDefaultParameters;

    @Before
    public void setUp() {
        Data key = mock(Data.class);
        Data otherKey = mock(Data.class);

        multiMapEventFilter = new MultiMapEventFilter(true, key);
        multiMapEventFilterSameAttributes = new MultiMapEventFilter(true, key);

        multiMapEventFilterOtherIncludeValue = new MultiMapEventFilter(false, key);
        multiMapEventFilterOtherKey = new MultiMapEventFilter(true, otherKey);
        multiMapEventFilterDefaultParameters = new MultiMapEventFilter();
    }

    @Test
    public void testEval() {
        assertFalse(multiMapEventFilter.eval(null));
    }

    @Test
    public void testEquals() {
        assertEquals(multiMapEventFilter, multiMapEventFilter);
        assertEquals(multiMapEventFilter, multiMapEventFilterSameAttributes);

        assertNotEquals(multiMapEventFilter, null);
        assertNotEquals(multiMapEventFilter, new Object());

        assertNotEquals(multiMapEventFilter, multiMapEventFilterOtherIncludeValue);
        assertNotEquals(multiMapEventFilter, multiMapEventFilterOtherKey);
        assertNotEquals(multiMapEventFilter, multiMapEventFilterDefaultParameters);
    }

    @Test
    public void testHashCode() {
        assertEquals(multiMapEventFilter.hashCode(), multiMapEventFilter.hashCode());
        assertEquals(multiMapEventFilter.hashCode(), multiMapEventFilterSameAttributes.hashCode());

        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterOtherIncludeValue.hashCode());
        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterOtherKey.hashCode());
        assertNotEquals(multiMapEventFilter.hashCode(), multiMapEventFilterDefaultParameters.hashCode());
    }
}
