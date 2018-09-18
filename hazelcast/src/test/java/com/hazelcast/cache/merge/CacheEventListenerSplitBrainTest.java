/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests different split-brain scenarios for {@link
 * ICache}.
 * <p> Most merge policies are tested with
 * {@link InMemoryFormat#BINARY} only, since they don't check the value.
 * <p>
 * The {@link
 * MergeIntegerValuesMergePolicy}
 * is tested with both in-memory formats, since it's using the value to
 * merge.
 * <p> The {@link DiscardMergePolicy}, {@link
 * PassThroughMergePolicy} and {@link PutIfAbsentMergePolicy} are also
 * tested with a data structure, which is only created in the smaller
 * cluster.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("WeakerAccess")
public class CacheEventListenerSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "format:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, PassThroughMergePolicy.class}
        });
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
        cacheConfig.setStatisticsEnabled(true);
        cacheConfig.setMergePolicy(mergePolicyClass.getName());

        MutableCacheEntryListenerConfiguration listenerConfiguration =
                new MutableCacheEntryListenerConfiguration(
                        FactoryBuilder.factoryOf(updatedListener), null, true, true);

        cacheConfig.addCacheEntryListenerConfiguration(listenerConfiguration);
        return cacheConfig;
    }

    protected CacheManager getCacheManager(HazelcastInstance instance) {
        HazelcastInstanceImpl hazelcastInstanceImpl = TestUtil.getHazelcastInstanceImpl(instance);
        HazelcastServerCachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstanceImpl);
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

        cacheA1 = newCacheIn(firstBrain[0], cacheNameA, cacheAUpdatedListener);
        cacheA2 = newCacheIn(secondBrain[0], cacheNameA, cacheAUpdatedListener);

        cacheB1 = newCacheIn(firstBrain[0], cacheNameB, cacheBUpdatedListener);
        cacheB2 = newCacheIn(secondBrain[0], cacheNameB, cacheBUpdatedListener);

        cacheA1.put("key", "same-value");
        cacheA2.put("key", "same-value");

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
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, cacheBUpdatedListener.updated.get());
            }
        },10);
    }

    static class TestCacheEntryUpdatedListener<K, V> implements CacheEntryUpdatedListener<K, V>, Serializable {

        public AtomicInteger updated = new AtomicInteger();

        public TestCacheEntryUpdatedListener() {
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
