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

import java.net.InetSocketAddress;
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
import com.hazelcast.client.connection.nio.ClientConnection;
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
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.diagnostics.AbstractMetricsIntegrationTest;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientMetricsTest extends AbstractMetricsIntegrationTest {

    private static final int STATS_PERIOD_SECONDS = 1;

    private static final String MAP_NAME = "StatTestMapFirst.First";
    private static final String CACHE_NAME = "StatTestICache.First";

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance hz;

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
        setCollectionContext(getNode(hz).nodeEngine.getMetricsRegistry().openContext(ProbeLevel.INFO));
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    /**
     * These are available even without client statistics being send as they reflect
     * information the member has about a {@link ClientEndpoint}.
     */
    @Test
    public void clientEndpointStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient();
        String uuid = getUuid(client);
        assertEventuallyHasStats(3, "client", uuid);
        String version = BuildInfoProvider.getBuildInfo().getVersion();
        ClientConnection ownerConnection = client.getConnectionManager().getOwnerConnection();
        InetSocketAddress clientAddress = ownerConnection.getLocalSocketAddress();
        String address = clientAddress.getAddress().getHostAddress() + ":" + clientAddress.getPort();
        assertHasStatsWith(1, "ns=client instance=" + uuid + " type=JAVA version=" + version
                + " target=" + address);
    }

    @Test
    public void clientCoreStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient(false);

        String uuid = getUuid(client);
        String prefix = "origin=" + uuid + " ";
        String instance = "instance=" + uuid;
        assertEventuallyHasStatsWith(54, prefix);
        assertEventuallyHasStatsWith(11, prefix + "ns=os ");
        assertEventuallyHasStatsWith(6,  prefix + "ns=runtime ");
        assertEventuallyHasStatsWith(12, prefix + "ns=memory ");
        assertEventuallyHasStatsWith(6,  prefix + "ns=gc ");
        assertEventuallyHasStatsWith(1,  prefix + "ns=client " + instance + " name=");
        assertEventuallyHasAllStats(
                prefix + "ns=client " + instance + " lastStatisticsCollectionTime",
                prefix + "ns=client " + instance + " clusterConnectionTimestamp",
                prefix + "ns=client " + instance + " enterprise");
    }

    @Test
    public void clientNearCacheStats() {
        final HazelcastClientInstanceImpl client = createHazelcastClient();
        produceSomeStats(client);

        String prefix = "origin=" + getUuid(client) + " ";
        assertEventuallyHasStatsWith(14,
                prefix + "ns=map.nearcache instance=" + MAP_NAME + " ");
        assertEventuallyHasStatsWith(14,
                prefix + "ns=cache.nearcache instance=/hz/" + CACHE_NAME + " ");
    }

    @Test
    public void multipleClientStats() {
        final HazelcastClientInstanceImpl client1 = createHazelcastClient();
        final HazelcastClientInstanceImpl client2 = createHazelcastClient();

        String prefix1 = "origin=" + getUuid(client1) + " ";
        String prefix2 = "origin=" + getUuid(client2) + " ";
        assertEventuallyHasStatsWith(54, prefix1);
        assertEventuallyHasStatsWith(54, prefix2);
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
        assertEventuallyHasStatsWith(54, prefix);
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
        return createHazelcastClient(true);
    }

    private HazelcastClientInstanceImpl createHazelcastClient(boolean nearCaches) {
        ClientConfig clientConfig = new ClientConfig()
                .setProperty(Statistics.ENABLED.getName(), "true")
                .setProperty(Statistics.PERIOD_SECONDS.getName(), Integer.toString(STATS_PERIOD_SECONDS))
                .setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name());
        if (nearCaches) {
                // add IMap and ICache with Near Cache config
            clientConfig
                .addNearCacheConfig(new NearCacheConfig(MAP_NAME))
                .addNearCacheConfig(new NearCacheConfig(CACHE_NAME));
        }

        clientConfig.getNetworkConfig().setConnectionAttemptLimit(20);

        HazelcastInstance clientInstance = hazelcastFactory.newHazelcastClient(clientConfig);
        return getHazelcastClientInstanceImpl(clientInstance);
    }

    protected HazelcastClientInstanceImpl getHazelcastClientInstanceImpl(HazelcastInstance client) {
        HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
        return clientProxy.client;
    }
}
