/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheContextTest extends HazelcastTestSupport {

    protected HazelcastInstance driverInstance;
    protected HazelcastInstance hazelcastInstance1;
    protected HazelcastInstance hazelcastInstance2;

    protected CachingProvider initAndGetCachingProvider() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        hazelcastInstance1 = factory.newHazelcastInstance();
        hazelcastInstance2 = factory.newHazelcastInstance();
        driverInstance = hazelcastInstance1;
        return HazelcastServerCachingProvider.createCachingProvider(driverInstance);
    }

    protected static class TestListener
            implements CacheEntryCreatedListener<String, String>, Serializable {

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
        }

    }

    protected CacheService getCacheService(HazelcastInstance instance) {
        return TestUtil.getNode(instance).getNodeEngine().getService(CacheService.SERVICE_NAME);
    }

    protected enum DecreaseType {

        DEREGISTER,
        SHUTDOWN,
        TERMINATE

    }

    protected void cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType decreaseType) {
        final String CACHE_NAME = "MyCache";
        final String CACHE_NAME_WITH_PREFIX = "/hz/" + CACHE_NAME;

        CachingProvider provider = initAndGetCachingProvider();
        CacheManager cacheManager = provider.getCacheManager();
        CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfig =
                new MutableCacheEntryListenerConfiguration<String, String>(
                        FactoryBuilder.factoryOf(new TestListener()), null, true, true);
        CompleteConfiguration<String, String> cacheConfig = new MutableConfiguration<String, String>();
        Cache<String, String> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.registerCacheEntryListener(cacheEntryListenerConfig);

        final CacheService cacheService1 = getCacheService(hazelcastInstance1);
        final CacheService cacheService2 = getCacheService(hazelcastInstance2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX));
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX));
            }
        });

        final CacheContext cacheContext1 = cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX);
        final CacheContext cacheContext2 = cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(cacheContext1.getCacheEntryListenerCount(), 1);
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(cacheContext2.getCacheEntryListenerCount(), 1);
            }
        });

        switch (decreaseType) {
            case DEREGISTER:
                cache.deregisterCacheEntryListener(cacheEntryListenerConfig);
                break;
            case SHUTDOWN:
                driverInstance.getLifecycleService().shutdown();
                break;
            case TERMINATE:
                driverInstance.getLifecycleService().terminate();
                break;
            default:
                throw new IllegalArgumentException("Unsupported decrease type: " + decreaseType);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(cacheContext1.getCacheEntryListenerCount(), 0);
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(cacheContext2.getCacheEntryListenerCount(), 0);
            }
        });
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterDeregister() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.DEREGISTER);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterShutdown() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.SHUTDOWN);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterTerminate() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.TERMINATE);
    }

}
