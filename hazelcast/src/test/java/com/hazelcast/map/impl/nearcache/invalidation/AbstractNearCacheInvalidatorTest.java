package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("ConstantConditions")
public class AbstractNearCacheInvalidatorTest {

    private static final String MAP_NAME = "myMap";
    private static final String SOURCE_UUID = UuidUtil.newUnsecureUuidString();

    private Data key;
    private TestNearCacheInvalidator invalidator;

    @Before
    public void setUp() {
        key = mock(Data.class);

        NodeEngine nodeEngine = mock(NodeEngine.class);

        invalidator = new TestNearCacheInvalidator(nodeEngine);
    }

    @Test(expected = AssertionError.class)
    public void invalidate_withNullKey() {
        invalidator.invalidate(null, MAP_NAME, SOURCE_UUID);
    }

    @Test(expected = AssertionError.class)
    public void invalidate_withNullMapName() {
        invalidator.invalidate(key, null, SOURCE_UUID);
    }

    @Test(expected = AssertionError.class)
    public void invalidate_withNullSourceUuid() {
        invalidator.invalidate(key, MAP_NAME, null);
    }

    @Test(expected = AssertionError.class)
    public void clear_withNullMapName() {
        invalidator.clear(null, SOURCE_UUID);
    }

    @Test(expected = AssertionError.class)
    public void clear_withNullSourceUuid() {
        invalidator.clear(MAP_NAME, null);
    }

    @Test
    public void canSendInvalidation_withInvalidFilter() {
        EventFilter filter = TrueEventFilter.INSTANCE;

        assertFalse(invalidator.canSendInvalidation(filter, SOURCE_UUID));
    }

    @Test
    public void canSendInvalidation_withInvalidEventType() {
        EventFilter filter = new EventListenerFilter(EntryEventType.ADDED.getType(), TrueEventFilter.INSTANCE);

        assertFalse(invalidator.canSendInvalidation(filter, SOURCE_UUID));
    }

    @Test
    public void canSendInvalidation_withInvalidSourceUuid() {
        UuidFilter uuidFilter = new UuidFilter(SOURCE_UUID);
        EventFilter filter = new EventListenerFilter(EntryEventType.INVALIDATION.getType(), uuidFilter);

        assertFalse(invalidator.canSendInvalidation(filter, SOURCE_UUID));
    }

    @Test
    public void canSendInvalidation() {
        UuidFilter uuidFilter = new UuidFilter(SOURCE_UUID);
        EventFilter filter = new EventListenerFilter(EntryEventType.INVALIDATION.getType(), uuidFilter);

        assertTrue(invalidator.canSendInvalidation(filter, UuidUtil.newUnsecureUuidString()));
    }

    @Test
    public void testDestroy() {
        invalidator.destroy(MAP_NAME, SOURCE_UUID);
    }

    @Test
    public void testReset() {
        invalidator.reset();
    }

    @Test
    public void testShutdown() {
        invalidator.shutdown();
    }

    private class TestNearCacheInvalidator extends AbstractNearCacheInvalidator {

        TestNearCacheInvalidator(NodeEngine nodeEngine) {
            super(nodeEngine);
        }

        @Override
        protected void invalidateInternal(Invalidation invalidation, int orderKey) {
        }
    }
}
