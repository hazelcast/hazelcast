package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheConfigTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testSetName_throwsException_whenNameNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setName(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetPredicate_throwsException_whenPredicateNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setPredicateConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBatchSize_throwsException_whenNotPositive() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBatchSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferSize_throwsException_whenNotPositive() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBufferSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDelaySeconds_throwsException_whenNegative() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setDelaySeconds(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetInMemoryFormat_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInMemoryFormat_throwsException_whenNative() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Test(expected = NullPointerException.class)
    public void testSetEvictionConfig_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setEvictionConfig(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerConfig_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.addEntryListenerConfig(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetEntryListenerConfigs_throwsException_whenNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setEntryListenerConfigs(null);
    }

    @Test
    public void testSetIndexConfigs_withNull() {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setIndexConfigs(null);

        assertNotNull(config.getIndexConfigs());
        assertTrue(config.getIndexConfigs().isEmpty());
    }

    @Test
    public void testToString() {
        QueryCacheConfig config = new QueryCacheConfig();

        assertNotNull(config.toString());
        assertTrue(config.toString().contains("QueryCacheConfig"));
    }
}
