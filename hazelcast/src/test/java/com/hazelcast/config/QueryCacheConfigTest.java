package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheConfigTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testSetName_throwsException_whenNameNull() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setName(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetPredicate_throwsException_whenPredicateNull() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setPredicateConfig(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBatchSize_throwsException_whenNotPositive() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBatchSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferSize_throwsException_whenNotPositive() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setBufferSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetDelaySeconds_throwsException_whenNegative() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setDelaySeconds(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testSetInMemoryFormat_throwsException_whenNull() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInMemoryFormat_throwsException_whenNative() throws Exception {
        QueryCacheConfig config = new QueryCacheConfig();
        config.setInMemoryFormat(InMemoryFormat.NATIVE);
    }
}