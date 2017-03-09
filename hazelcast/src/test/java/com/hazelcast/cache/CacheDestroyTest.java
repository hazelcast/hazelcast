/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheDestroyTest extends CacheTestSupport {
    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);
    private HazelcastInstance[] hazelcastInstances;
    private HazelcastInstance hazelcastInstance;

    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

    @Override
    protected void onSetup() {
        hazelcastInstances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < hazelcastInstances.length; i++) {
            hazelcastInstances[i] = factory.newHazelcastInstance(createConfig());
        }
        warmUpPartitions(hazelcastInstances);
        waitAllForSafeState(hazelcastInstances);
        hazelcastInstance = hazelcastInstances[0];
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hazelcastInstances = null;
        hazelcastInstance = null;
    }


    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setBackupCount(INSTANCE_COUNT - 1);
        return cacheConfig;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Test
    public void test_cacheDestroyOperation() throws ExecutionException, InterruptedException {
        final String CACHE_NAME = "MyCache";
        final String FULL_CACHE_NAME = HazelcastCacheManager.CACHE_MANAGER_PREFIX + CACHE_NAME;

        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(getHazelcastInstance());
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(CACHE_NAME, new CacheConfig());

        NodeEngineImpl nodeEngine1 = getNode(getHazelcastInstance()).getNodeEngine();
        final ICacheService cacheService1 = nodeEngine1.getService(ICacheService.SERVICE_NAME);
        InternalOperationService operationService1 = nodeEngine1.getOperationService();

        NodeEngineImpl nodeEngine2 = getNode(hazelcastInstances[1]).getNodeEngine();
        final ICacheService cacheService2 = nodeEngine2.getService(ICacheService.SERVICE_NAME);

        assertNotNull(cacheService1.getCacheConfig(FULL_CACHE_NAME));
        assertNotNull(cacheService2.getCacheConfig(FULL_CACHE_NAME));

        // Invoke on single node and the operation is also forward to others nodes by the operation itself
        operationService1.invokeOnTarget(ICacheService.SERVICE_NAME,
                new CacheDestroyOperation(FULL_CACHE_NAME),
                nodeEngine1.getThisAddress());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cacheService1.getCacheConfig(FULL_CACHE_NAME));
                assertNull(cacheService2.getCacheConfig(FULL_CACHE_NAME));
            }
        });
    }

    @Test
    public void testInvalidationListenerCallCount() {
        final ICache<String, String> cache = createCache();

        final AtomicInteger counter = new AtomicInteger(0);

        final CacheConfig config = cache.getConfiguration(CacheConfig.class);

        registerInvalidationListener(new CacheEventListener() {
            @Override
            public void handleEvent(Object eventObject) {
                if (eventObject instanceof Invalidation) {
                    Invalidation event = (Invalidation) eventObject;
                    if (null == event.getKey() && config.getNameWithPrefix().equals(event.getName())) {
                        counter.incrementAndGet();
                    }
                }
            }
        }, config.getNameWithPrefix());

        cache.destroy();

        // Make sure that at least 1 invalidation event has been received
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() >= 1);
            }
        }, 2);
        // Make sure that no more than INSTNACE_COUNT events are received ever
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() <= INSTANCE_COUNT);
            }
        }, 3);

    }

    private void registerInvalidationListener(CacheEventListener cacheEventListener, String name) {
        HazelcastInstanceProxy hzInstance = (HazelcastInstanceProxy) this.hazelcastInstance;
        hzInstance.getOriginal().node.getNodeEngine().getEventService()
                .registerListener(ICacheService.SERVICE_NAME, name, cacheEventListener);
    }

}
