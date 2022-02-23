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

import com.hazelcast.cache.impl.CacheContext;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
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

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheContextTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "MyCache";
    private static final String CACHE_NAME_WITH_PREFIX = "/hz/" + CACHE_NAME;

    protected HazelcastInstance driverInstance;
    protected HazelcastInstance hazelcastInstance1;
    protected HazelcastInstance hazelcastInstance2;
    protected CachingProvider provider;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        hazelcastInstance1 = factory.newHazelcastInstance();
        hazelcastInstance2 = factory.newHazelcastInstance();

        driverInstance = hazelcastInstance1;
        provider = createServerCachingProvider(driverInstance);
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

    protected void cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType decreaseType) {
        CacheManager cacheManager = provider.getCacheManager();
        CacheEntryListenerConfiguration<String, String> cacheEntryListenerConfig
                = new MutableCacheEntryListenerConfiguration<String, String>(
                FactoryBuilder.factoryOf(new TestListener()), null, true, true);
        CompleteConfiguration<String, String> cacheConfig = new MutableConfiguration<String, String>();
        Cache<String, String> cache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        cache.registerCacheEntryListener(cacheEntryListenerConfig);

        final CacheService cacheService1 = getCacheService(hazelcastInstance1);
        final CacheService cacheService2 = getCacheService(hazelcastInstance2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX));
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNotNull(cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX));
            }
        });

        final CacheContext cacheContext1 = cacheService1.getCacheContext(CACHE_NAME_WITH_PREFIX);
        final CacheContext cacheContext2 = cacheService2.getCacheContext(CACHE_NAME_WITH_PREFIX);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, cacheContext1.getCacheEntryListenerCount());
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, cacheContext2.getCacheEntryListenerCount());
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
            public void run() {
                assertEquals(0, cacheContext1.getCacheEntryListenerCount());
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, cacheContext2.getCacheEntryListenerCount());
            }
        });
    }

    private CacheService getCacheService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(CacheService.SERVICE_NAME);
    }

    protected enum DecreaseType {
        DEREGISTER,
        SHUTDOWN,
        TERMINATE
    }

    public static class TestListener implements CacheEntryCreatedListener<String, String>, Serializable {

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
        }
    }
}
