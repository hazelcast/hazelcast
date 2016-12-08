package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NullQueryCacheTest extends HazelcastTestSupport {

    @Test
    public void testGet() throws Exception {
        assertNull(NULL_QUERY_CACHE.get(1));
    }

    @Test
    public void testContainsKey() throws Exception {
        assertFalse(NULL_QUERY_CACHE.containsKey(1));
    }

    @Test
    public void testContainsValue() throws Exception {
        assertFalse(NULL_QUERY_CACHE.containsValue(1));
    }

    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(NULL_QUERY_CACHE.isEmpty());
    }

    @Test
    public void testSize() throws Exception {
        assertEquals(0, NULL_QUERY_CACHE.size());
    }

    @Test
    public void testGetAll() throws Exception {
        assertNull(NULL_QUERY_CACHE.getAll(null));
    }

    @Test
    public void testKeySet() throws Exception {
        assertNull(NULL_QUERY_CACHE.keySet());
    }

    @Test
    public void testEntrySet() throws Exception {
        assertNull(NULL_QUERY_CACHE.entrySet());
    }

    @Test
    public void testValues() throws Exception {
        assertNull(NULL_QUERY_CACHE.values());
    }

    @Test
    public void testGetName() throws Exception {
        assertNull(NULL_QUERY_CACHE.getName());
    }
}
