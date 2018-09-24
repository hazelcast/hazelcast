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

package com.hazelcast.client.statistics;

import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.statistics.Statistics;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICacheManager;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.internal.diagnostics.AbstractMetricsTest;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeRenderContext;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientMetricsTest extends AbstractMetricsTest {

    private static final int STATS_PERIOD_SECONDS = 1;

    private static final String MAP_NAME = "StatTestMapFirst.First";
    private static final String CACHE_NAME = "StatTestICache.First";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance hz;
    private ProbeRenderContext renderContext;

    /**
     * In order to get the TCP statistics we need a real {@link ConnectionManager}.
     */
    @Rule
    public final OverridePropertyRule useRealNetwork = set(HAZELCAST_TEST_USE_NETWORK, "true");

    @Before
    public void setup() {
        newMember();
    }

    private void newMember() {
        hz = hazelcastFactory.newHazelcastInstance();
        renderContext = getNode(hz).nodeEngine.getProbeRegistry().newRenderContext();
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Override
    protected ProbeRenderContext getRenderContext() {
        return renderContext;
    }

    /**
     * These are available even without client statistics being send as they reflect
     * information the member has about a {@link ClientEndpoint}.
     */
    @Test
    public void clientEndpointStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient();
        assertHasStatsEventually(3, "client", getUuid(client));
    }

    @Test
    public void clientCoreStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient();

        String prefix = "origin=" + getUuid(client) + " ";
        assertHasStatsEventually(56, prefix);
        assertHasStatsEventually(11, prefix + "os.");
        assertHasStatsEventually(6,  prefix + "runtime.");
        assertHasStatsEventually(12, prefix + "memory.");
        assertHasStatsEventually(6,  prefix + "gc.");
        assertHasStatsEventually(1,  prefix + "type=JAVA instance=");
        assertHasAllStatsEventually(
                prefix + "lastStatisticsCollectionTime",
                prefix + "clusterConnectionTimestamp",
                prefix + "enterprise");
    }

    @Test
    public void clientNearCacheStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient();
        produceSomeStats(client);

        String prefix = "origin=" + getUuid(client) + " ";
        assertHasStatsEventually(14,
                prefix + "type=map instance=" + MAP_NAME + " nearcache.");
        assertHasStatsEventually(14,
                prefix + "type=cache instance=/hz/" + CACHE_NAME + " nearcache.");
    }

    @Test
    public void multipleClientStats() {
        final HazelcastClientInstanceImpl client1 = createHazelcastClient();
        final HazelcastClientInstanceImpl client2 = createHazelcastClient();

        String prefix1 = "origin=" + getUuid(client1) + " ";
        String prefix2 = "origin=" + getUuid(client2) + " ";
        assertHasStatsEventually(56, prefix1);
        assertHasStatsEventually(56, prefix2);
    }

    @Test
    public void clusterReconnect() {
        HazelcastClientInstanceImpl client = createHazelcastClient();

        hz.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    latch.countDown();
                }
            }
        });

        newMember();

        assertOpenEventually(latch);

        String prefix = "origin=" + getUuid(client) + " ";
        assertHasStatsEventually(56, prefix);
    }

    private void produceSomeStats(HazelcastClientInstanceImpl client) {
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        map.put(5, 10);
        assertEquals(10, map.get(5).intValue());
        assertEquals(10, map.get(5).intValue());

        ICache<Integer, Integer> cache = createCache(CACHE_NAME, client);
        cache.put(9, 20);
        assertEquals(20, cache.get(9).intValue());
        assertEquals(20, cache.get(9).intValue());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ICache<Integer, Integer> createCache(String cacheName, HazelcastInstance client) {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hz);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.createCache(cacheName,
                new CacheConfig().setInMemoryFormat(InMemoryFormat.BINARY));
        ICacheManager clientCacheManager = client.getCacheManager();
        return clientCacheManager.getCache(cacheName);
    }

    private String getUuid(final HazelcastClientInstanceImpl client) {
        return client.getClientClusterService().getLocalClient().getUuid();
    }

    private HazelcastClientInstanceImpl createHazelcastClient() {
        ClientConfig clientConfig = new ClientConfig()
                .setProperty(Statistics.ENABLED.getName(), "true")
                .setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS))
                .setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name())
                // add IMap and ICache with Near Cache config
                .addNearCacheConfig(new NearCacheConfig(MAP_NAME))
                .addNearCacheConfig(new NearCacheConfig(CACHE_NAME));

        clientConfig.getNetworkConfig().setConnectionAttemptLimit(20);

        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);
        return getHazelcastClientInstanceImpl(clientInstance);
    }

    protected HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        return clientProxy.client;
    }
}
