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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unchecked")
public class CacheSplitBrainTest extends SplitBrainTestSupport {

    @Parameterized.Parameters(name = "mergePolicy:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                LatestAccessCacheMergePolicy.class,
                HigherHitsCacheMergePolicy.class,
                PutIfAbsentCacheMergePolicy.class,
                PassThroughCacheMergePolicy.class,
                CustomCacheMergePolicy.class
        });
    }

    @Parameterized.Parameter
    public Class<? extends CacheMergePolicy> mergePolicyClass;

    private String cacheName = randomMapName();
    private Cache cache1;
    private Cache cache2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected int[] brains() {
        // second half should merge to first 
        return new int[]{2, 1};
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) throws Exception {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain)
            throws Exception {

        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        CacheConfig cacheConfig = newCacheConfig(cacheName, mergePolicyClass);
        cache1 = createCache(firstBrain[0], cacheConfig);
        cache2 = createCache(secondBrain[0], cacheConfig);

        if (mergePolicyClass == LatestAccessCacheMergePolicy.class) {
            afterSplitLatestAccessCacheMergePolicy();
        }
        if (mergePolicyClass == HigherHitsCacheMergePolicy.class) {
            afterSplitHigherHitsCacheMergePolicy();
        }
        if (mergePolicyClass == PutIfAbsentCacheMergePolicy.class) {
            afterSplitPutIfAbsentCacheMergePolicy();
        }
        if (mergePolicyClass == PassThroughCacheMergePolicy.class) {
            afterSplitPassThroughCacheMergePolicy();
        }
        if (mergePolicyClass == CustomCacheMergePolicy.class) {
            afterSplitCustomCacheMergePolicy();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) throws Exception {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == LatestAccessCacheMergePolicy.class) {
            afterMergeLatestAccessCacheMergePolicy();
        }
        if (mergePolicyClass == HigherHitsCacheMergePolicy.class) {
            afterMergeHigherHitsCacheMergePolicy();
        }
        if (mergePolicyClass == PutIfAbsentCacheMergePolicy.class) {
            afterMergePutIfAbsentCacheMergePolicy();
        }
        if (mergePolicyClass == PassThroughCacheMergePolicy.class) {
            afterMergePassThroughCacheMergePolicy();
        }
        if (mergePolicyClass == CustomCacheMergePolicy.class) {
            afterMergeCustomCacheMergePolicy();
        }
    }

    private void afterSplitLatestAccessCacheMergePolicy() {
        cache1.put("key1", "value");
        assertEquals("value", cache1.get("key1")); // Access to record

        // Prevent updating at the same time
        sleepAtLeastMillis(100);

        cache2.put("key1", "LatestAccessedValue");
        assertEquals("LatestAccessedValue", cache2.get("key1")); // Access to record

        cache2.put("key2", "value2");
        assertEquals("value2", cache2.get("key2")); // Access to record

        // Prevent updating at the same time
        sleepAtLeastMillis(100);

        cache1.put("key2", "LatestAccessedValue2");
        assertEquals("LatestAccessedValue2", cache1.get("key2")); // Access to record
    }

    private void afterMergeLatestAccessCacheMergePolicy() {
        assertEquals("LatestAccessedValue", cache1.get("key1"));
        assertEquals("LatestAccessedValue", cache2.get("key1"));

        assertEquals("LatestAccessedValue2", cache1.get("key2"));
        assertEquals("LatestAccessedValue2", cache2.get("key2"));
    }

    private void afterSplitHigherHitsCacheMergePolicy() {
        cache1.put("key1", "higherHitsValue");
        cache1.put("key2", "value2");

        // Increase hits number
        assertEquals("higherHitsValue", cache1.get("key1"));
        assertEquals("higherHitsValue", cache1.get("key1"));

        cache2.put("key1", "value1");
        cache2.put("key2", "higherHitsValue2");

        // Increase hits number
        assertEquals("higherHitsValue2", cache2.get("key2"));
        assertEquals("higherHitsValue2", cache2.get("key2"));
    }

    private void afterMergeHigherHitsCacheMergePolicy() {
        assertEquals("higherHitsValue", cache1.get("key1"));
        assertEquals("higherHitsValue", cache2.get("key1"));

        assertEquals("higherHitsValue2", cache1.get("key2"));
        assertEquals("higherHitsValue2", cache2.get("key2"));
    }

    private void afterSplitPutIfAbsentCacheMergePolicy() {
        cache1.put("key1", "PutIfAbsentValue1");

        cache2.put("key1", "value");
        cache2.put("key2", "PutIfAbsentValue2");
    }

    private void afterMergePutIfAbsentCacheMergePolicy() {
        assertEquals("PutIfAbsentValue1", cache1.get("key1"));
        assertEquals("PutIfAbsentValue1", cache2.get("key1"));

        assertEquals("PutIfAbsentValue2", cache1.get("key2"));
        assertEquals("PutIfAbsentValue2", cache2.get("key2"));
    }

    private void afterSplitPassThroughCacheMergePolicy() {
        cache1.put("key", "value");
        cache2.put("key", "passThroughValue");
    }

    private void afterMergePassThroughCacheMergePolicy() {
        assertEquals("passThroughValue", cache1.get("key"));
        assertEquals("passThroughValue", cache2.get("key"));
    }

    private void afterSplitCustomCacheMergePolicy() {
        cache1.put("key", "value");
        cache2.put("key", 1);
    }

    private void afterMergeCustomCacheMergePolicy() {
        assertEquals(1, cache1.get("key"));
        assertEquals(1, cache2.get("key"));
    }

    private static Cache createCache(HazelcastInstance hazelcastInstance, CacheConfig cacheConfig) {
        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        return cacheManager1.createCache(cacheConfig.getName(), cacheConfig);
    }

    private static CacheConfig newCacheConfig(String cacheName, Class<? extends CacheMergePolicy> mergePolicy) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setMergePolicy(mergePolicy.getName());
        return cacheConfig;
    }

    private static class CustomCacheMergePolicy implements CacheMergePolicy {

        @Override
        public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
            if (mergingEntry.getValue() instanceof Integer) {
                return mergingEntry.getValue();
            }
            return null;
        }
    }

    private static class MergeLifecycleListener implements LifecycleListener {
        private final CountDownLatch latch;

        MergeLifecycleListener(int mergingClusterSize) {
            latch = new CountDownLatch(mergingClusterSize);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }

        void await() {
            assertOpenEventually(latch);
        }
    }
}
