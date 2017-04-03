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
package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.cache.CacheUtil;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that Near Cache invalidation events are delivered when a cache is:
 * <ul>
 * <li><code>clear</code>ed</li>
 * <li><code>destroy</code>ed (with <code>Cache.destroy()</code></li>
 * <li>destroyed by its client-side <code>CacheManager.destroyCache</code></li>
 * </ul>
 * and <b>not delivered</b> when a cache is closed (<code>Cache.close()</code>).
 * Respective operations are tested when executed either from member or client-side Cache proxies with the exception of
 * CacheManager.destroyCache, in which case the Near Cache is already destroyed on the client-side and the listener
 * registration is removed <b>before</b> the invocation for destroying the cache is sent to the member, so no event can be
 * received.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({SlowTest.class})
public class ClientNearCacheInvalidationTest extends HazelcastTestSupport {

    static final String DEFAULT_CACHE_NAME = "com.hazelcast.client.cache.impl.nearcache.ClientNearCacheInvalidationTest";

    // time to wait until invalidation event is delivered (when used with assertTrueEventually)
    // and time to wait when testing that no invalidation event is delivered (used with assertTrueAllTheTime)
    static final int TIMEOUT = 5;

    // some events are delivered exactly once, some are delivered MEMBER_COUNT times
    // we start MEMBER_COUNT members in the test and validate count of events against this number
    static final int MEMBER_COUNT = 2;

    // when true, invoke operations which are supposed to deliver invalidation events from a Cache instance on a member
    // otherwise use the client-side proxy.
    @Parameter
    public boolean invokeCacheOperationsFromMember;

    @Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "fromMember:{0}, format:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, InMemoryFormat.BINARY},
                {false, InMemoryFormat.OBJECT},
                {true, InMemoryFormat.BINARY},
                {true, InMemoryFormat.OBJECT},
        });
    }

    private HazelcastInstance member;
    private TestHazelcastFactory hazelcastFactory;
    private NearCacheTestContext testContext;

    @Before
    public void setup() {
        hazelcastFactory = new TestHazelcastFactory();
        member = hazelcastFactory.newHazelcastInstance(getConfig());
        if (MEMBER_COUNT > 1) {
            HazelcastInstance[] allMembers = new HazelcastInstance[MEMBER_COUNT];
            allMembers[0] = member;
            for (int i = 1; i < MEMBER_COUNT; i++) {
                allMembers[i] = hazelcastFactory.newHazelcastInstance(getConfig());
            }
            waitAllForSafeState(allMembers);
        }
        ClientConfig clientConfig = createClientConfig();
        clientConfig.addNearCacheConfig(createNearCacheConfig(inMemoryFormat));
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) HazelcastServerCachingProvider
                .createCachingProvider(member).getCacheManager();

        ICache<Object, String> cache = cacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(inMemoryFormat));
        ICache<Object, String> memberCache = member.getCacheManager().getCache(
                CacheUtil.getPrefixedCacheName(DEFAULT_CACHE_NAME, null, null));

        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(
                cacheManager.getCacheNameWithPrefix(DEFAULT_CACHE_NAME));

        testContext = new NearCacheTestContext(client, member, cacheManager, memberCacheManager, nearCacheManager, cache,
                memberCache, nearCache);

        // make sure several partitions are populated with data
        for (int i = 0; i < 1000; i++) {
            testContext.memberCache.put(Integer.toString(i), Integer.toString(i));
        }
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void when_cacheDestroyed_invalidationEventIsReceived() {
        AtomicInteger counter = new AtomicInteger();
        registerInvalidationListener(counter);
        destroy();
        assertInvalidationEventCountNeverExceeds(counter, MEMBER_COUNT);
    }

    @Test
    public void when_cacheCleared_invalidationEventIsReceived() {
        AtomicInteger counter = new AtomicInteger();
        registerInvalidationListener(counter);
        clear();
        assertInvalidationEventCountNeverExceeds(counter, 1);
    }

    @Test
    public void when_cacheClosed_invalidationEventIsNotReceived() {
        AtomicInteger counter = new AtomicInteger();
        registerInvalidationListener(counter);
        close();
        assertInvalidationEventNeverReceived(counter);
    }

    /**
     * When CacheManager.destroyCache() is invoked from client-side CacheManager, an invalidation event is received.
     * When invoked from a member-side CacheManager, invalidation event is not received.
     */
    @Test
    public void when_cacheManagerDestroyCacheInvoked_invalidationEventMayBeReceived() {
        AtomicInteger counter = new AtomicInteger();
        registerInvalidationListener(counter);
        destroyCacheFromCacheManager();
        if (invokeCacheOperationsFromMember) {
            // when from a member-side cache manager, invalidation event cannot be received, as the invalidation listener
            // registration is removed *before* the invocation to destroy the cache is sent to the member
            assertInvalidationEventNeverReceived(counter);
        } else {
            // when from a client-side cache manager, an invalidation event is received
            assertInvalidationEventCountNeverExceeds(counter, MEMBER_COUNT);
        }
    }

    @Test
    public void when_shuttingDown_invalidationEventIsNotReceived() {
        AtomicInteger counter = new AtomicInteger();
        registerInvalidationListener(counter);
        shutdown();
        assertInvalidationEventNeverReceived(counter);
    }

    /**
     * Asserts that the counter is eventually incremented to at least 1 and stays less than or equal to maximumEventCount.
     */
    private void assertInvalidationEventCountNeverExceeds(final AtomicInteger counter, final int maximumEventCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue("At least one invalidation event should have been received", counter.get() >= 1);
            }
        }, TIMEOUT);
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue("Expected invalidation event to be received at most " + maximumEventCount + " times",
                        counter.get() <= maximumEventCount);
            }
        }, TIMEOUT);
    }

    private void assertInvalidationEventNeverReceived(final AtomicInteger counter) {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, counter.get());
            }
        }, TIMEOUT);
    }

    private void registerInvalidationListener(AtomicInteger counter) {
        EventHandler handler = new NearCacheRepairingHandler(counter);
        ListenerMessageCodec listenerCodec = createInvalidationListenerCodec();
        final HazelcastClientInstanceImpl clientInstance = testContext.client.client;
        clientInstance.getListenerService().registerListener(listenerCodec, handler);
    }

    private ListenerMessageCodec createInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return CacheAddInvalidationListenerCodec.encodeRequest(CacheUtil.getDistributedObjectName(DEFAULT_CACHE_NAME),
                        localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return CacheAddInvalidationListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(CacheUtil.getDistributedObjectName(DEFAULT_CACHE_NAME),
                        realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    private void clear() {
        if (invokeCacheOperationsFromMember) {
            testContext.memberCache.clear();
        } else {
            testContext.cache.clear();
        }
    }

    private void close() {
        if (invokeCacheOperationsFromMember) {
            testContext.memberCache.close();
        } else {
            testContext.cache.close();
        }
    }

    private void destroy() {
        if (invokeCacheOperationsFromMember) {
            testContext.memberCache.destroy();
        } else {
            testContext.cache.destroy();
        }
    }

    private void shutdown() {
        if (invokeCacheOperationsFromMember) {
            testContext.member.shutdown();
        } else {
            testContext.client.shutdown();
        }
    }

    private void destroyCacheFromCacheManager() {
        if (invokeCacheOperationsFromMember) {
            testContext.memberCacheManager.destroyCache(DEFAULT_CACHE_NAME);
        } else {
            testContext.cacheManager.destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return new NearCacheConfig()
                .setInMemoryFormat(inMemoryFormat)
                .setName(DEFAULT_CACHE_NAME);
    }

    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig().setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1);
    }

    private final class NearCacheRepairingHandler
            extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final AtomicInteger counter;

        NearCacheRepairingHandler(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void handle(String name, Data key, String sourceUuid, UUID partitionUuid, long sequence) {
            counter.incrementAndGet();
        }

        @Override
        public void handle(String name, Collection<Data> keys, Collection<String> sourceUuids, Collection<UUID> partitionUuids,
                           Collection<Long> sequences) {
            counter.incrementAndGet();
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }
}
