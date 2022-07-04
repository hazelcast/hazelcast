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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class CacheEventListenerSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, PassThroughMergePolicy.class},
                {OBJECT, PassThroughMergePolicy.class}
        });
    }

    @Override
    protected Config config() {
        return super.config();
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class<? extends SplitBrainMergePolicy> mergePolicyClass;

    protected String cacheNameA = "cache-A";
    protected String cacheNameB = "cache-B";
    protected ICache<Object, Object> cacheA1;
    protected ICache<Object, Object> cacheA2;
    protected ICache<Object, Object> cacheB1;
    protected ICache<Object, Object> cacheB2;
    protected MergeLifecycleListener mergeLifecycleListener;
    private TestCacheEntryUpdatedListener cacheAUpdatedListener = new TestCacheEntryUpdatedListener();
    private TestCacheEntryUpdatedListener cacheBUpdatedListener = new TestCacheEntryUpdatedListener();

    private CacheConfig newCacheConfig(final TestCacheEntryUpdatedListener updatedListener) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        if (inMemoryFormat == NATIVE) {
            cacheConfig.getEvictionConfig().setMaxSizePolicy(USED_NATIVE_MEMORY_SIZE);
        }
        cacheConfig.getMergePolicyConfig().setPolicy(mergePolicyClass.getName());

        if (updatedListener != null) {
            MutableCacheEntryListenerConfiguration listenerConfiguration =
                    new MutableCacheEntryListenerConfiguration(
                            FactoryBuilder.factoryOf(updatedListener), null, true, true);

            cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);
        }

        return cacheConfig;
    }

    protected CacheManager getCacheManager(HazelcastInstance instance) {
        HazelcastInstanceImpl hazelcastInstanceImpl = TestUtil.getHazelcastInstanceImpl(instance);
        HazelcastServerCachingProvider cachingProvider = createServerCachingProvider(hazelcastInstanceImpl);
        return cachingProvider.getCacheManager();
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        // cacheA2 will merge into cacheA1
        cacheA1 = newCacheIn(firstBrain[0], cacheNameA, cacheAUpdatedListener);
        cacheA2 = newCacheIn(secondBrain[0], cacheNameA, null);

        cacheA1.put("key", "same-value");
        cacheA2.put("key", "same-value");

        // cacheB2 will merge into cacheB1
        cacheB1 = newCacheIn(firstBrain[0], cacheNameB, cacheBUpdatedListener);
        cacheB2 = newCacheIn(secondBrain[0], cacheNameB, null);

        cacheB1.put("key", "old-value");
        cacheB2.put("key", "updated-value");
    }

    private ICache<Object, Object> newCacheIn(HazelcastInstance instance,
                                              String cacheName, TestCacheEntryUpdatedListener updatedListener) {
        return (ICache<Object, Object>) getCacheManager(instance).createCache(cacheName, newCacheConfig(updatedListener));
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        cacheA1 = instances[0].getCacheManager().getCache(cacheNameA);

        assert_no_update_event_generated_on_merge_of_equal_entries();
        assert_update_event_generated_on_merge_of_not_equal_entries();
    }

    private void assert_no_update_event_generated_on_merge_of_equal_entries() {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, cacheAUpdatedListener.updated.get());
            }
        }, 5);
    }

    private void assert_update_event_generated_on_merge_of_not_equal_entries() {
        assertTrueEventually(() -> assertEquals(1, cacheBUpdatedListener.updated.get()));
    }

    static class TestCacheEntryUpdatedListener<K, V> implements CacheEntryUpdatedListener<K, V>, Serializable {

        public AtomicInteger updated = new AtomicInteger();

        TestCacheEntryUpdatedListener() {
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : cacheEntryEvents) {
                updated.incrementAndGet();
            }
        }
    }
}
