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

package com.hazelcast.client.cache.stats;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.stats.CacheStatsTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheStatsTest extends CacheStatsTest {

    @Parameter
    public boolean nearCacheEnabled;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @Parameters(name = "nearCached:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                Boolean.TRUE,
                Boolean.FALSE,
        });
    }

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
        return createClientCachingProvider(client);
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        if (nearCacheEnabled) {
            clientConfig.addNearCacheConfig(new NearCacheConfig("*"));
        }
        return clientConfig;
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereIsNoBackup() {
        expectedException.expect(UnsupportedOperationException.class);
        super.testOwnedEntryCountWhenThereIsNoBackup();
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnStaticCluster() {
        expectedException.expect(UnsupportedOperationException.class);
        super.testOwnedEntryCountWhenThereAreBackupsOnStaticCluster();
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster() {
        expectedException.expect(UnsupportedOperationException.class);
        super.testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster();
    }

    @Override
    @Test
    public void testExpirations() {
        expectedException.expect(UnsupportedOperationException.class);
        super.testExpirations();
    }

    @Override
    @Test
    public void testEvictions() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        expectedException.expect(UnsupportedOperationException.class);
        stats.getCacheEvictions();
    }

    @Override
    @Test
    public void testNearCacheStats_availableWhenEnabled() {
        if (nearCacheEnabled) {
            testNearCacheStats_whenNearCacheEnabled();
        } else {
            expectedException.expect(UnsupportedOperationException.class);
            testNearCacheStats_whenNearCacheDisabled();
        }
    }

    // throws UnsupportedOperationException
    private void testNearCacheStats_whenNearCacheDisabled() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        stats.getNearCacheStatistics();
    }

    private void testNearCacheStats_whenNearCacheEnabled() {
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
