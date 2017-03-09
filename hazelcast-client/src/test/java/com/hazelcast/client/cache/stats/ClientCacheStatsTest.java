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

package com.hazelcast.client.cache.stats;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.stats.CacheStatsTest;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheStatsTest extends CacheStatsTest {

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Override
    protected void onSetup() {
        super.onSetup();
        getHazelcastInstance();
        ClientConfig clientConfig = createClientConfig();
        client = instanceFactory.newHazelcastClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        instanceFactory.shutdownAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instanceFactory.newHazelcastInstance(createConfig());
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return HazelcastClientCachingProvider.createCachingProvider(client);
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testOwnedEntryCountWhenThereIsNoBackup() {
        super.testOwnedEntryCountWhenThereIsNoBackup();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testOwnedEntryCountWhenThereAreBackupsOnStaticCluster() {
        super.testOwnedEntryCountWhenThereAreBackupsOnStaticCluster();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster() {
        super.testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testExpirations() {
        super.testExpirations();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void testEvictions() {
        super.testEvictions();
    }

    @Test
    public void testNearCacheStatsWhenNearCacheEnabled() {
        String cacheName = randomName();
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setName(cacheName);
        ClientConfig clientConfig = ((HazelcastClientProxy) client).getClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig().setName(cacheName));
        ICache<Integer, String> cache = createCache(cacheName, cacheConfig);
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertNotNull(stats.getNearCacheStatistics());
    }

}
