package com.hazelcast.map.impl.event;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test EntryEventDataCache implementations
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryEventDataCacheTest {

    private EntryEventDataCache instance;

    private static final Address ADDRESS;

    static {
        Address address = null;
        try {
            address = new Address("127.0.0.1", 5701);
        } catch (UnknownHostException e) {
        }
        ADDRESS = address;
    }

    @Parameterized.Parameter
    public FilteringStrategy filteringStrategy;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        // setup mock MapServiceContext & NodeEngine, required by FilteringStrategy's
        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        NodeEngine mockNodeEngine = mock(NodeEngine.class);
        when(mockNodeEngine.getThisAddress()).thenReturn(ADDRESS);
        when(mapServiceContext.toData(anyObject())).thenReturn(new HeapData());
        when(mapServiceContext.getNodeEngine()).thenReturn(mockNodeEngine);

        return Arrays.asList(new Object[][] {
                {new DefaultEntryEventFilteringStrategy(null, mapServiceContext)},
                {new QueryCacheNaturalFilteringStrategy(null, mapServiceContext)},
        });
    }

    @Before
    public void setup() {
        instance = filteringStrategy.getEntryEventDataCache();
    }

    @Test
    public void getOrCreateEventDataIncludingValues_whenAlreadyCached() throws Exception {
        // when: creating EntryEventData including values with same arguments
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        EntryEventData shouldBeCached = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: returned instances are the same
        assertSame(eed, shouldBeCached);
    }

    @Test
    public void getOrCreateEventDataExcludingValues_whenAlreadyCached() throws Exception {
        // when: creating EntryEventData including values with same arguments
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        EntryEventData shouldBeCached = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: returned instances are the same
        assertSame(eed, shouldBeCached);
    }

    @Test
    public void isEmpty_whenNoEntryEventDataHaveBeenCreated() throws Exception {
        // when: no EntryEventData have been getOrCreate'd
        // then: the cache is empty
        assertTrue(instance.isEmpty());
    }

    @Test
    public void isEmpty_whenEntryEventDataHaveBeenCreated() throws Exception {
        // when: EntryEventData have been getOrCreate'd
        instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: the cache is not empty
        assertFalse(instance.isEmpty());
    }

    @Test
    public void eventDataIncludingValues_whenValueIsCached() throws Exception {
        // when: EntryEventData including values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: the cache is not empty & eventDataIncludingValues returns the cached entry
        assertFalse(instance.isEmpty());
        assertSame(eed, instance.eventDataIncludingValues().iterator().next());
    }

    @Test
    public void eventDataIncludingValues_whenNoValuesCached() throws Exception {
        // when: EntryEventData including values have been created

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.isEmpty());
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataIncludingValues_whenDataExcludingValuesAreCached() throws Exception {
        // when: EntryEventData excluding values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: eventDataIncludingValues returns null or empty collection
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataExcludingValues_whenValueIsCached() throws Exception {
        // when: EntryEventData excluding values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: the cache is not empty & eventDataExcludingValues returns the cached entry
        assertFalse(instance.isEmpty());
        assertSame(eed, instance.eventDataExcludingValues().iterator().next());
    }

    @Test
    public void eventDataExcludingValues_whenNoValuesCached() throws Exception {
        // when: no EntryEventData values have been created

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.isEmpty());
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataExcludingValues_whenDataIncludingValuesAreCached() throws Exception {
        // when: no EntryEventData values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.eventDataExcludingValues() == null || instance.eventDataExcludingValues().isEmpty());
    }
}
