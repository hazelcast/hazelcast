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

import com.hazelcast.cache.impl.CacheIterator;
import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheClusterWideIteratorTest extends HazelcastTestSupport {

    private CachingProvider cachingProvider;

    @Parameters(name = "prefetchValues:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false},
                {true},
        });
    }

    @Parameter(0)
    public boolean prefetchValues;

    protected Iterator getIterator(Cache cache) {
        return new CacheIterator((CacheProxy) cache, prefetchValues);
    }

    @Before
    public void init() {
        cachingProvider = createCachingProvider();
    }

    protected CachingProvider createCachingProvider() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        return createServerCachingProvider(hazelcastInstance);
    }

    @After
    public void tear() {
        cachingProvider.close();
    }

    protected CacheConfig createCacheConfig() {
        return new CacheConfig();
    }

    private Cache<Integer, Integer> getCache() {
        String cacheName = randomString();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CacheConfig<Integer, Integer> config = createCacheConfig();
        return cacheManager.createCache(cacheName, config);
    }

    @Test
    public void testIterator() {
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) getCache();
        int size = 1111;
        int multiplier = 11;
        for (int i = 0; i < size; i++) {
            cache.put(i, i * multiplier);
        }

        int[] keys = new int[size];
        int k = 0;
        Iterator<Cache.Entry<Integer, Integer>> iter = getIterator(cache);
        while (iter.hasNext()) {
            Cache.Entry<Integer, Integer> e = iter.next();
            int key = e.getKey();
            int value = e.getValue();
            assertEquals(key * multiplier, value);
            keys[k++] = key;
        }
        assertEquals(size, k);

        Arrays.sort(keys);
        for (int i = 0; i < size; i++) {
            assertEquals(i, keys[i]);
        }
    }

    @Test
    public void testIteratorRemove() {
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) getCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iter = getIterator(cache);
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        assertEquals(0, cache.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorIllegalRemove() {
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) getCache();
        int size = 10;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iter = getIterator(cache);
        if (iter.hasNext()) {
            iter.remove();
        }
    }

}
