package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NonStopInvalidatorTest {

    private Data key;
    private NonStopInvalidator invalidator;

    @Before
    public void setUp() {
        key = mock(Data.class);

        NodeEngine nodeEngine = mock(NodeEngine.class);

        MapServiceContext context = mock(MapServiceContext.class);
        when(context.getNodeEngine()).thenReturn(nodeEngine);

        NearCacheProvider nearCacheProvider = mock(NearCacheProvider.class);

        invalidator = new NonStopInvalidator(context, nearCacheProvider);
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testInvalidate_withInvalidMapName() {
        invalidator.invalidate(key, null, "anySourceUuid");
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testInvalidate_withInvalidSourceUuid() {
        invalidator.invalidate(key, "anyMapName", null);
    }
}
