package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheSimpleConfigTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void givenCacheLoaderIsConfigured_whenConfigureCacheLoaderFactory_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheLoader("foo");

        expectedException.expect(IllegalStateException.class);
        config.setCacheLoaderFactory("bar");
    }

    @Test
    public void givenCacheLoaderFactoryIsConfigured_whenConfigureCacheLoader_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheLoaderFactory("bar");

        expectedException.expect(IllegalStateException.class);
        config.setCacheLoader("foo");
    }

    @Test
    public void givenCacheWriterIsConfigured_whenConfigureCacheWriterFactory_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheWriter("foo");

        expectedException.expect(IllegalStateException.class);
        config.setCacheWriterFactory("bar");
    }

    @Test
    public void givenCacheWriterFactoryIsConfigured_whenConfigureCacheWriter_thenThrowIllegalStateException() {
        CacheSimpleConfig config = new CacheSimpleConfig();
        config.setCacheWriterFactory("bar");

        expectedException.expect(IllegalStateException.class);
        config.setCacheWriter("foo");
    }
}
