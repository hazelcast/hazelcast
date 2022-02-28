/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.spi.CachingProvider;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheResourceTest
        extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = new TestHazelcastInstanceFactory(2);
    }

    @After
    public void tear() {
        factory.shutdownAll();
    }

    @Test
    public void testCloseableCacheLoader() throws InterruptedException {
        CachingProvider provider =
                createServerCachingProvider(factory.newHazelcastInstance());

        CacheManager cacheManager = provider.getCacheManager();

        CloseableCacheLoader loader = new CloseableCacheLoader();

        Factory<CloseableCacheLoader> loaderFactory = FactoryBuilder.factoryOf(loader);
        CompleteConfiguration<Object, Object> configuration =
                new CacheConfig()
                        .setCacheLoaderFactory(loaderFactory)
                        .setReadThrough(true);

        Cache<Object, Object> cache = cacheManager.createCache("test", configuration);

        // trigger partition assignment
        cache.get("key");

        factory.newHazelcastInstance();

        for (int i = 0; i < 1000; i++) {
            cache.get(i);
            LockSupport.parkNanos(1000);
        }

        assertFalse("CacheLoader should not be closed!", loader.closed);
    }

    private static class CloseableCacheLoader implements CacheLoader, Closeable, Serializable {

        private volatile boolean closed = false;

        @Override
        public Object load(Object key) throws CacheLoaderException {
            if (closed) {
                throw new IllegalStateException();
            }
            return null;
        }

        @Override
        public Map loadAll(Iterable keys) throws CacheLoaderException {
            if (closed) {
                throw new IllegalStateException();
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

    }

    @Test
    public void testCloseableCacheWriter() throws InterruptedException {
        CachingProvider provider =
                createServerCachingProvider(factory.newHazelcastInstance());

        CacheManager cacheManager = provider.getCacheManager();

        CloseableCacheWriter writer = new CloseableCacheWriter();

        Factory<CloseableCacheWriter> writerFactory = FactoryBuilder.factoryOf(writer);
        CompleteConfiguration<Object, Object> configuration =
                new CacheConfig()
                        .setCacheWriterFactory(writerFactory)
                        .setWriteThrough(true);

        Cache<Object, Object> cache = cacheManager.createCache("test", configuration);

        // trigger partition assignment
        cache.get("key");

        factory.newHazelcastInstance();

        for (int i = 0; i < 1000; i++) {
            cache.put(i, i);
            LockSupport.parkNanos(1000);
        }

        assertFalse("CacheWriter should not be closed!", writer.closed);
    }

    private static class CloseableCacheWriter implements CacheWriter, Closeable, Serializable {

        private volatile boolean closed = false;

        @Override
        public void write(Cache.Entry entry) throws CacheWriterException {
            if (closed) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            if (closed) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void deleteAll(Collection keys) throws CacheWriterException {
            if (closed) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void writeAll(Collection collection) throws CacheWriterException {
            if (closed) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

    }

    @Test
    public void testCloseableCacheListener() {
        CachingProvider provider = createServerCachingProvider(factory.newHazelcastInstance());

        CacheManager cacheManager = provider.getCacheManager();

        CloseableListener listener = new CloseableListener();

        Factory<CloseableListener> listenerFactory = FactoryBuilder.factoryOf(listener);
        CompleteConfiguration<Object, Object> configuration =
                new CacheConfig()
                        .addCacheEntryListenerConfiguration(
                                new MutableCacheEntryListenerConfiguration(listenerFactory, null, true, false));

        Cache<Object, Object> cache = cacheManager.createCache("test", configuration);
        cache.close();

        assertTrue("CloseableListener.close() should be called when cache is closed!", listener.closed);
    }

    private static class CloseableListener implements CacheEntryCreatedListener, Closeable, Serializable {

        private volatile boolean closed = false;

        @Override
        public void onCreated(Iterable iterable) throws CacheEntryListenerException {
            if (closed) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

    }

}
