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

package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.ClientCacheProxy;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.internal.adapter.ICacheDataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContext;
import com.hazelcast.internal.nearcache.impl.NearCacheTestContextBuilder;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.CacheUtil.getPrefixedCacheName;
import static com.hazelcast.client.cache.impl.nearcache.ClientCacheInvalidationListener.createInvalidationEventHandler;
import static com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport.generateValueFromKey;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that Near Cache invalidation events are delivered when a cache is:
 * <ul>
 * <li>{@code cleared}</li>
 * <li>{@code destroyed} (via {@link ICache#destroy()})</li>
 * <li>{@code destroyed} (via its client-side {@link CacheManager#destroyCache(String)})</li>
 * </ul>
 * and <b>not delivered</b> when a cache is closed (via {@link ICache#close()}).
 * <p>
 * Respective operations are tested when executed either from member or client-side Cache proxies with the exception of
 * {@link CacheManager#destroyCache(String)}, in which case the Near Cache is already destroyed on the client-side and
 * the listener registration is removed <b>before</b> the invocation for destroying the cache is sent to the member,
 * so no event can be received.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheInvalidationTest extends HazelcastTestSupport {

    private static final String DEFAULT_CACHE_NAME = "ClientCacheNearCacheInvalidationTest";

    // time to wait until invalidation event is delivered (when used with assertTrueEventually)
    // and time to wait when testing that no invalidation event is delivered (used with assertTrueAllTheTime)
    private static final int TIMEOUT = 10;

    // some events are delivered exactly once, some are delivered MEMBER_COUNT times
    // we start MEMBER_COUNT members in the test and validate count of events against this number
    private static final int MEMBER_COUNT = 2;

    private static final int INITIAL_POPULATION_COUNT = 1000;

    @Parameters(name = "format:{0}, fromMember:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},

                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    // when true, invoke operations which are supposed to deliver invalidation events from a Cache instance on a member
    // otherwise use the client-side proxy.
    @Parameter(1)
    public boolean invokeCacheOperationsFromMember;

    private TestHazelcastFactory hazelcastFactory;
    private NearCacheTestContext<Integer, String, Object, String> testContext;
    private ClientCacheInvalidationListener invalidationListener;

    @Before
    public void setup() {
        hazelcastFactory = new TestHazelcastFactory();

        HazelcastInstance[] allMembers = new HazelcastInstance[MEMBER_COUNT];
        for (int i = 0; i < MEMBER_COUNT; i++) {
            // every instance should have its own getConfig() call because an existing EE test relies on this
            Config config = getConfig()
                    .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "false");
            allMembers[i] = hazelcastFactory.newHazelcastInstance(config);
        }
        waitAllForSafeState(allMembers);

        NearCacheConfig nearCacheConfig = getNearCacheConfig(inMemoryFormat);
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider provider = createClientCachingProvider(client);
        HazelcastServerCachingProvider memberProvider = createServerCachingProvider(allMembers[0]);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();

        CacheConfig<Integer, String> cacheConfig = getCacheConfig(inMemoryFormat);
        ICache<Integer, String> cache = cacheManager.createCache(DEFAULT_CACHE_NAME, cacheConfig);
        ICache<Integer, String> memberCache = memberCacheManager.getCache(getPrefixedCacheName(DEFAULT_CACHE_NAME, null, null));

        NearCacheManager nearCacheManager = ((ClientCacheProxy) cache).getContext().getNearCacheManager(cache.getServiceName());
        NearCache<Object, String> nearCache = nearCacheManager.getNearCache(
                cacheManager.getCacheNameWithPrefix(DEFAULT_CACHE_NAME));

        SerializationService serializationService = client.getSerializationService();

        invalidationListener = createInvalidationEventHandler(cache);

        NearCacheTestContextBuilder<Integer, String, Object, String> builder
                = new NearCacheTestContextBuilder<>(nearCacheConfig, serializationService);
        testContext = builder
                .setDataInstance(allMembers[0])
                .setNearCacheInstance(client)
                .setDataAdapter(new ICacheDataStructureAdapter<>(memberCache))
                .setNearCacheAdapter(new ICacheDataStructureAdapter<>(cache))
                .setMemberCacheManager(memberCacheManager)
                .setCacheManager(cacheManager)
                .setNearCacheManager(nearCacheManager)
                .setNearCache(nearCache)
                .build();
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void putToCacheAndGetInvalidationEventWhenNodeShutdown() {
        Config config = getConfig()
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE));

        HazelcastInstance instanceToShutdown = hazelcastFactory.newHazelcastInstance(config);

        warmUpPartitions(testContext.dataInstance, instanceToShutdown);
        waitAllForSafeState(testContext.dataInstance, instanceToShutdown);

        NearCacheConfig nearCacheConfig = getNearCacheConfig(inMemoryFormat)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(LocalUpdatePolicy.CACHE_ON_UPDATE);

        CacheConfig<String, String> cacheConfig = getCacheConfig(inMemoryFormat);
        final NearCacheTestContext<String, String, Object, String> nearCacheTestContext1
                = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig, cacheConfig);
        final NearCacheTestContext<String, String, Object, String> nearCacheTestContext2
                = createNearCacheTest(DEFAULT_CACHE_NAME, nearCacheConfig, cacheConfig);

        Map<String, String> keyAndValues = new HashMap<>();

        // put cache record from client-1 to instance which is going to be shutdown
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            String key = generateKeyOwnedBy(instanceToShutdown);
            String value = generateValueFromKey(i);
            nearCacheTestContext1.nearCacheAdapter.put(key, value);
            keyAndValues.put(key, value);
        }

        // verify that records are exist at Near Cache of client-1 because `local-update-policy` is `CACHE_ON_UPDATE`
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            String key = entry.getKey();
            String exceptedValue = entry.getValue();
            String actualValue = getFromNearCache(nearCacheTestContext1, key);
            assertEquals(exceptedValue, actualValue);
        }

        // remove records through client-2 so there will be invalidation events
        // to send to client to invalidate its Near Cache
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            nearCacheTestContext2.nearCacheAdapter.remove(entry.getKey());
        }

        // we don't shutdown the instance because in case of shutdown even though events are published to event queue,
        // they may not be processed in the event queue due to shutdown event queue executor or may not be sent
        // to client endpoint due to IO handler shutdown

        // for not to making test fragile, we just simulate shutting down by sending its event through `LifeCycleService`,
        // so the node should flush invalidation events before shutdown
        ((LifecycleServiceImpl) instanceToShutdown.getLifecycleService())
                .fireLifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN);

        // verify that records in the Near Cache of client-1 are invalidated eventually when instance shutdown
        for (Map.Entry<String, String> entry : keyAndValues.entrySet()) {
            final String key = entry.getKey();
            assertTrueEventually(() -> assertNull(getFromNearCache(nearCacheTestContext1, key)));
        }
    }

    @Test
    public void putToCacheAndDoNotInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled() {
        // we need to use another cache name, to get the invalidation setting working
        String cacheName = "disabledPerEntryInvalidationCache";

        NearCacheConfig nearCacheConfig = getNearCacheConfig(inMemoryFormat)
                .setName(cacheName)
                .setInvalidateOnChange(true);

        CacheConfig<Integer, String> cacheConfig = getCacheConfig(inMemoryFormat);
        cacheConfig.setName(cacheName);
        cacheConfig.setDisablePerEntryInvalidationEvents(true);

        final NearCacheTestContext<Integer, String, Object, String> nearCacheTestContext1
                = createNearCacheTest(cacheName, nearCacheConfig, cacheConfig);
        final NearCacheTestContext<Integer, String, Object, String> nearCacheTestContext2
                = createNearCacheTest(cacheName, nearCacheConfig, cacheConfig);

        // put cache record from client-1
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            nearCacheTestContext1.nearCacheAdapter.put(i, generateValueFromKey(i));
        }

        // get records from client-2
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            final Integer key = i;
            final String value = nearCacheTestContext2.nearCacheAdapter.get(key);
            // records are stored in the cache as async not sync, so these records will be there in cache eventually
            assertTrueEventually(() -> assertEquals(value, getFromNearCache(nearCacheTestContext2, key)));
        }

        // update cache record from client-1
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            // update the cache records with new values
            nearCacheTestContext1.nearCacheAdapter.put(i, generateValueFromKey(i + INITIAL_POPULATION_COUNT));
        }

        int invalidationEventFlushFreq = Integer.parseInt(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getDefaultValue());
        // wait some time and if there are invalidation events to be sent in batch
        // (we assume that they should be flushed, received and processed in this time window already)
        sleepSeconds(2 * invalidationEventFlushFreq);

        // get records from client-2
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            String actualValue = nearCacheTestContext2.nearCacheAdapter.get(i);
            String expectedValue = generateValueFromKey(i);
            // verify that still we have old records in the Near Cache, because, per entry invalidation events are disabled
            assertEquals(expectedValue, actualValue);
        }

        nearCacheTestContext1.nearCacheAdapter.clear();

        // can't get expired records from client-2
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            final int key = i;
            // records are stored in the Near Cache will be invalidated eventually, since cache records are cleared
            // because we just disable per entry invalidation events, not full-flush events
            assertTrueEventually(() -> assertNull(getFromNearCache(nearCacheTestContext2, key)));
        }
    }

    @Test
    public void when_shuttingDown_invalidationEventIsNotReceived() {
        populateMemberCache();

        if (invokeCacheOperationsFromMember) {
            testContext.dataInstance.shutdown();
        } else {
            testContext.nearCacheInstance.shutdown();
        }

        assertNoFurtherInvalidation();
    }

    @Test
    public void when_cacheDestroyed_invalidationEventIsReceived() {
        populateMemberCache();

        if (invokeCacheOperationsFromMember) {
            testContext.dataAdapter.destroy();
        } else {
            testContext.nearCacheAdapter.destroy();
        }

        assertLeastInvalidationCount(1);
    }

    @Test
    public void when_cacheCleared_invalidationEventIsReceived() {
        populateMemberCache();

        if (invokeCacheOperationsFromMember) {
            testContext.dataAdapter.clear();
        } else {
            testContext.nearCacheAdapter.clear();
        }

        assertNoFurtherInvalidationThan(1);
    }

    @Test
    public void when_cacheClosed_invalidationEventIsNotReceived() {
        populateMemberCache();

        if (invokeCacheOperationsFromMember) {
            testContext.dataAdapter.close();
        } else {
            testContext.nearCacheAdapter.close();
        }

        assertNoFurtherInvalidation();
    }

    /**
     * When CacheManager.destroyCache() is invoked from client-side CacheManager, an invalidation event is received.
     * When invoked from a member-side CacheManager, invalidation event is not received.
     */
    @Test
    public void when_cacheManagerDestroyCacheInvoked_invalidationEventMayBeReceived() {
        populateMemberCache();

        if (invokeCacheOperationsFromMember) {
            testContext.memberCacheManager.destroyCache(DEFAULT_CACHE_NAME);
        } else {
            testContext.cacheManager.destroyCache(DEFAULT_CACHE_NAME);
        }

        assertLeastInvalidationCount(1);
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        return clientConfig;
    }

    protected NearCacheConfig getNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat);
    }

    protected <K, V> CacheConfig<K, V> getCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig<K, V>()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1);
    }

    private <K, V, NK, NV> NearCacheTestContext<K, V, NK, NV> createNearCacheTest(String cacheName,
                                                                                  NearCacheConfig nearCacheConfig,
                                                                                  CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig()
                // disable reconciliation, without reconciliation tests should pass
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), "0")
                .addNearCacheConfig(nearCacheConfig);

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        CachingProvider provider = createClientCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();

        ICache<K, V> cache = cacheManager.createCache(cacheName, cacheConfig);
        NearCacheManager nearCacheManager = ((ClientCacheProxy) cache).getContext().getNearCacheManager(CacheService.SERVICE_NAME);
        NearCache<NK, NV> nearCache = nearCacheManager.getNearCache(cacheManager.getCacheNameWithPrefix(cacheName));

        NearCacheTestContextBuilder<K, V, NK, NV> builder = new NearCacheTestContextBuilder<>(nearCacheConfig,
                client.getSerializationService());
        return builder
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<>(cache))
                .setNearCacheManager(nearCacheManager)
                .setNearCache(nearCache)
                .build();
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setName(DEFAULT_CACHE_NAME);
    }

    private void populateMemberCache() {
        // make sure several partitions are populated with data
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            testContext.dataAdapter.put(i, Integer.toString(i));
        }
        waitForInvalidationFromInitialPopulation();
    }

    private void waitForInvalidationFromInitialPopulation() {
        assertTrueEventually(() -> {
            long invalidationCountFromStats = testContext.stats.getInvalidationRequests();
            assertEquals(format("Expected %d Near Cache invalidations received by stats, but found %d (%s)",
                    INITIAL_POPULATION_COUNT, invalidationCountFromStats, testContext.stats),
                    INITIAL_POPULATION_COUNT, invalidationCountFromStats);

            long invalidationCountFromListener = invalidationListener.getInvalidationCount();
            assertEquals(format("Expected %d Near Cache invalidations received by listener, but found %d",
                    INITIAL_POPULATION_COUNT, invalidationCountFromListener),
                    INITIAL_POPULATION_COUNT, invalidationCountFromListener);

            testContext.stats.resetInvalidationEvents();
            invalidationListener.resetInvalidationCount();
        });
    }

    private void assertNoFurtherInvalidation() {
        assertNoFurtherInvalidationThan(0);
    }

    private void assertNoFurtherInvalidationThan(final int expectedInvalidationCount) {
        AssertTask assertTask = () -> {
            long invalidationCount = invalidationListener.getInvalidationCount();
            assertTrue(format("Expected no further Near Cache invalidation events than %d, but received %d (%s)\n"
                            + "(%s)", expectedInvalidationCount, invalidationCount, testContext.stats,
                    String.join("\n", invalidationListener.getSingleInvalidationEventsLog())),
                    invalidationCount <= expectedInvalidationCount);
        };

        assertTrueEventually(assertTask);
        assertTrueAllTheTime(assertTask, TIMEOUT);
        invalidationListener.resetInvalidationCount();
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLeastInvalidationCount(final int leastInvalidationCount) {
        AssertTask assertTask = () -> {
            long invalidationCount = invalidationListener.getInvalidationCount();
            assertTrue(format("Expected at least %d Near Cache invalidation events, but received %d (%s)",
                    leastInvalidationCount, invalidationCount, testContext.stats),
                    invalidationCount >= leastInvalidationCount);
        };

        assertTrueEventually(assertTask);
        assertTrueAllTheTime(assertTask, TIMEOUT);
        invalidationListener.resetInvalidationCount();
    }

    @SuppressWarnings("unchecked")
    private static <K, V, NK, NV> NV getFromNearCache(NearCacheTestContext<K, V, NK, NV> nearCacheTestContext, Object key) {
        if (nearCacheTestContext.nearCacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            key = nearCacheTestContext.serializationService.toData(key);
        }
        return nearCacheTestContext.nearCache.get((NK) key);
    }
}
