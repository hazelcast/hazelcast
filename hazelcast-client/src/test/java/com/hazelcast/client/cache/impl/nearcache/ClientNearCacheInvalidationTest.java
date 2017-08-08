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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
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
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.cache.CacheUtil.getPrefixedCacheName;
import static com.hazelcast.client.cache.nearcache.ClientCacheInvalidationListener.createInvalidationEventHandler;
import static com.hazelcast.spi.properties.GroupProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static java.util.Arrays.asList;
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
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(SlowTest.class)
@SuppressWarnings({"ConstantConditions", "WeakerAccess"})
public class ClientNearCacheInvalidationTest extends HazelcastTestSupport {

    static final String DEFAULT_CACHE_NAME = "com.hazelcast.client.cache.impl.nearcache.ClientNearCacheInvalidationTest";

    // time to wait until invalidation event is delivered (when used with assertTrueEventually)
    // and time to wait when testing that no invalidation event is delivered (used with assertTrueAllTheTime)
    static final int TIMEOUT = 10;

    // some events are delivered exactly once, some are delivered MEMBER_COUNT times
    // we start MEMBER_COUNT members in the test and validate count of events against this number
    static final int MEMBER_COUNT = 2;

    static final int INITIAL_POPULATION_COUNT = 1000;

    // when true, invoke operations which are supposed to deliver invalidation events from a Cache instance on a member
    // otherwise use the client-side proxy.
    @Parameter
    public boolean invokeCacheOperationsFromMember;

    @Parameter(1)
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "fromMember:{0}, format:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false, InMemoryFormat.BINARY},
                {false, InMemoryFormat.OBJECT},
                {true, InMemoryFormat.BINARY},
                {true, InMemoryFormat.OBJECT},
        });
    }

    private TestHazelcastFactory hazelcastFactory;
    private NearCacheTestContext testContext;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        Config config = getConfig();
        ClientConfig clientConfig = createClientConfig()
                .addNearCacheConfig(createNearCacheConfig(inMemoryFormat));

        hazelcastFactory = new TestHazelcastFactory();

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        if (MEMBER_COUNT > 1) {
            HazelcastInstance[] allMembers = new HazelcastInstance[MEMBER_COUNT];
            allMembers[0] = member;
            for (int i = 1; i < MEMBER_COUNT; i++) {
                allMembers[i] = hazelcastFactory.newHazelcastInstance(config);
            }
            waitAllForSafeState(allMembers);
        }

        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);
        NearCacheManager nearCacheManager = client.client.getNearCacheManager();
        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastServerCachingProvider memberProvider = HazelcastServerCachingProvider.createCachingProvider(member);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        HazelcastServerCacheManager memberCacheManager = (HazelcastServerCacheManager) memberProvider.getCacheManager();

        ICache<Object, String> cache = cacheManager.createCache(DEFAULT_CACHE_NAME, createCacheConfig(inMemoryFormat));
        ICache<Object, String> memberCache = memberCacheManager.getCache(getPrefixedCacheName(DEFAULT_CACHE_NAME, null, null));

        NearCache<Object, String> nearCache = nearCacheManager.getNearCache(
                cacheManager.getCacheNameWithPrefix(DEFAULT_CACHE_NAME));

        testContext = new NearCacheTestContext(client, member, cacheManager, memberCacheManager, nearCacheManager, cache,
                memberCache, nearCache, createInvalidationEventHandler(cache));

        // make sure several partitions are populated with data
        for (int i = 0; i < INITIAL_POPULATION_COUNT; i++) {
            testContext.memberCache.put(Integer.toString(i), Integer.toString(i));
        }
    }

    @Override
    protected Config getConfig() {
        return super.getConfig()
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "false");
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void when_shuttingDown_invalidationEventIsNotReceived() {
        waitEndOfInvalidationsFromInitialPopulation();

        shutdown();

        assertNoFurtherInvalidation();
    }

    @Test
    public void when_cacheDestroyed_invalidationEventIsReceived() {
        waitEndOfInvalidationsFromInitialPopulation();

        destroy();

        assertLeastInvalidationCount(1);
    }

    @Test
    public void when_cacheCleared_invalidationEventIsReceived() {
        waitEndOfInvalidationsFromInitialPopulation();

        clear();

        assertNoFurtherInvalidationThan(1);
    }

    @Test
    public void when_cacheClosed_invalidationEventIsNotReceived() {
        waitEndOfInvalidationsFromInitialPopulation();

        close();

        assertNoFurtherInvalidation();
    }

    /**
     * When CacheManager.destroyCache() is invoked from client-side CacheManager, an invalidation event is received.
     * When invoked from a member-side CacheManager, invalidation event is not received.
     */
    @Test
    public void when_cacheManagerDestroyCacheInvoked_invalidationEventMayBeReceived() {
        waitEndOfInvalidationsFromInitialPopulation();

        destroyCacheFromCacheManager();

        if (invokeCacheOperationsFromMember) {
            assertNoFurtherInvalidation();
        } else {
            assertNoFurtherInvalidationThan(MEMBER_COUNT);
        }
    }

    private void waitEndOfInvalidationsFromInitialPopulation() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long invalidationCount = testContext.invalidationListener.getInvalidationCount();
                assertEquals(INITIAL_POPULATION_COUNT, invalidationCount);
                testContext.invalidationListener.resetInvalidationCount();
            }
        });
    }

    private void assertNoFurtherInvalidation() {
        assertNoFurtherInvalidationThan(0);
    }

    private void assertNoFurtherInvalidationThan(final int expectedInvalidationCount) {
        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() throws Exception {
                long invalidationCount = testContext.invalidationListener.getInvalidationCount();
                assertEquals(expectedInvalidationCount, invalidationCount);
            }
        };

        assertTrueAllTheTime(assertTask, TIMEOUT);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLeastInvalidationCount(final int leastInvalidationCount) {
        AssertTask assertTask = new AssertTask() {
            @Override
            public void run() throws Exception {
                long invalidationCount = testContext.invalidationListener.getInvalidationCount();
                assertTrue("invalidationCount = [" + invalidationCount + "] " +
                        "but should be >= [" + leastInvalidationCount + "]", invalidationCount >= leastInvalidationCount);
            }
        };

        assertTrueAllTheTime(assertTask, TIMEOUT);
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
}
