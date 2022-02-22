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

import com.hazelcast.cache.impl.MXBeanUtil;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheManagementTest extends CacheTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
    private HazelcastInstance hazelcastInstance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        hazelcastInstance = factory.newHazelcastInstance(config);
    }

    @Override
    protected void onTearDown() {
        factory.shutdownAll();
        hazelcastInstance = null;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    private void doCacheManagement(boolean enable, boolean stats) {
        String cacheName = randomName();
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setManagementEnabled(false);
        cacheConfig.setStatisticsEnabled(false);
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(cacheName, cacheConfig);
        if (enable) {
            if (stats) {
                cacheManager.enableStatistics(cacheName, true);
            } else {
                cacheManager.enableManagement(cacheName, true);
            }
        }
        if (enable) {
            assertTrue(MXBeanUtil.isRegistered("hazelcast", cacheName, stats));
        } else {
            assertFalse(MXBeanUtil.isRegistered("hazelcast", cacheName, stats));
        }
    }

    @Test
    public void cacheMXBeanShouldBeRegisteredWhenManagementIsEnabled() {
        doCacheManagement(true, false);
    }

    @Test
    public void cacheMXBeanShouldNotBeRegisteredWhenManagementIsNotEnabled() {
        doCacheManagement(false, false);
    }

    @Test
    public void cacheMXBeanShouldBeRegisteredWhenStatisticsIsEnabled() {
        doCacheManagement(true, true);
    }

    @Test
    public void cacheMXBeanShouldNotBeRegisteredWhenStatisticsIsNotEnabled() {
        doCacheManagement(false, true);
    }

}
