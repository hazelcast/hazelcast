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

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("unchecked")
public class LegacyCacheSplitBrainTest extends SplitBrainTestSupport {

    @Parameters(name = "inMemoryFormat:{0}, mergePolicy:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY, LatestAccessCacheMergePolicy.class},
                {BINARY, HigherHitsCacheMergePolicy.class},
                {BINARY, PutIfAbsentCacheMergePolicy.class},
                {BINARY, PassThroughCacheMergePolicy.class},
                {BINARY, CustomCacheMergePolicy.class}
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class<? extends CacheMergePolicy> mergePolicyClass;

    private String cacheName = randomMapName();
    private Cache cache1;
    private Cache cache2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        CacheConfig cacheConfig = newCacheConfig(cacheName, mergePolicyClass, inMemoryFormat);
        cache1 = createCache(firstBrain[0], cacheConfig);
        cache2 = createCache(secondBrain[0], cacheConfig);

        if (mergePolicyClass == LatestAccessCacheMergePolicy.class) {
            afterSplitLatestAccessCacheMergePolicy();
        } else if (mergePolicyClass == HigherHitsCacheMergePolicy.class) {
            afterSplitHigherHitsCacheMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentCacheMergePolicy.class) {
            afterSplitPutIfAbsentCacheMergePolicy();
        } else if (mergePolicyClass == PassThroughCacheMergePolicy.class) {
            afterSplitPassThroughCacheMergePolicy();
        } else if (mergePolicyClass == CustomCacheMergePolicy.class) {
            afterSplitCustomCacheMergePolicy();
        } else {
            fail();
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        if (mergePolicyClass == LatestAccessCacheMergePolicy.class) {
            afterMergeLatestAccessCacheMergePolicy();
        } else if (mergePolicyClass == HigherHitsCacheMergePolicy.class) {
            afterMergeHigherHitsCacheMergePolicy();
        } else if (mergePolicyClass == PutIfAbsentCacheMergePolicy.class) {
            afterMergePutIfAbsentCacheMergePolicy();
        } else if (mergePolicyClass == PassThroughCacheMergePolicy.class) {
            afterMergePassThroughCacheMergePolicy();
        } else if (mergePolicyClass == CustomCacheMergePolicy.class) {
            afterMergeCustomCacheMergePolicy();
        } else {
            fail();
        }
    }

    private void afterSplitLatestAccessCacheMergePolicy() {
        cache1.put("key1", "value");
        // access to record
        assertEquals("value", cache1.get("key1"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cache2.put("key1", "LatestAccessedValue");
        // access to record
        assertEquals("LatestAccessedValue", cache2.get("key1"));

        cache2.put("key2", "value2");
        // access to record
        assertEquals("value2", cache2.get("key2"));

        // prevent updating at the same time
        sleepAtLeastMillis(100);

        cache1.put("key2", "LatestAccessedValue2");
        // access to record
        assertEquals("LatestAccessedValue2", cache1.get("key2"));
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

        // increase hits number
        assertEquals("higherHitsValue", cache1.get("key1"));
        assertEquals("higherHitsValue", cache1.get("key1"));

        cache2.put("key1", "value1");
        cache2.put("key2", "higherHitsValue2");

        // increase hits number
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
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        return cacheManager.createCache(cacheConfig.getName(), cacheConfig);
    }

    protected CacheConfig newCacheConfig(String cacheName, Class<? extends CacheMergePolicy> mergePolicy,
                                         InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setMergePolicy(mergePolicy.getName());
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        return cacheConfig;
    }

    protected static class CustomCacheMergePolicy implements CacheMergePolicy {

        @Override
        public Object merge(String cacheName, CacheEntryView mergingEntry, CacheEntryView existingEntry) {
            if (mergingEntry.getValue() instanceof Integer) {
                return mergingEntry.getValue();
            }
            return null;
        }
    }
}
