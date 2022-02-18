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

package com.hazelcast.client.cache.impl;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.cache.impl.ClientCacheHelper.createCacheConfig;
import static com.hazelcast.client.cache.impl.ClientCacheHelper.enableStatisticManagementOnNodes;
import static com.hazelcast.client.cache.impl.ClientCacheHelper.getCacheConfig;
import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheHelperTest extends HazelcastTestSupport {

    private static final String CACHE_NAME = "fullCacheName";
    private static final String SIMPLE_CACHE_NAME = "cacheName";

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastClientInstanceImpl client;
    private HazelcastClientInstanceImpl exceptionThrowingClient;

    private CacheConfig<String, String> newCacheConfig;

    private CacheConfig<String, String> cacheConfig;
    private ConcurrentMap<String, CacheConfig> configs;

    @Before
    public void setUp() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName(SIMPLE_CACHE_NAME);

        Config config = new Config();
        config.addCacheConfig(cacheSimpleConfig);

        hazelcastFactory.newHazelcastInstance(config);
        client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());

        exceptionThrowingClient = mock(HazelcastClientInstanceImpl.class, RETURNS_DEEP_STUBS);
        when(exceptionThrowingClient.getClientPartitionService()).thenThrow(new IllegalArgumentException("expected"));

        newCacheConfig = new CacheConfig<String, String>(CACHE_NAME);

        cacheConfig = new CacheConfig<String, String>(CACHE_NAME);
        configs = new ConcurrentHashMap<String, CacheConfig>(singletonMap(CACHE_NAME, cacheConfig));
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientCacheHelper.class);
    }

    @Test
    public void testGetCacheConfig() {
        CacheConfig<String, String> cacheConfig = getCacheConfig(client, CACHE_NAME, CACHE_NAME);

        assertNull(cacheConfig);
    }

    @Test
    public void testGetCacheConfig_withSimpleCacheName() {
        CacheConfig<String, String> cacheConfig = getCacheConfig(client, SIMPLE_CACHE_NAME, SIMPLE_CACHE_NAME);

        assertNotNull(cacheConfig);
        assertEquals(SIMPLE_CACHE_NAME, cacheConfig.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetCacheConfig_rethrowsExceptions() {
        getCacheConfig(exceptionThrowingClient, CACHE_NAME, "simpleCacheName");
    }

    @Test
    public void testCreateCacheConfig_whenSyncCreate_thenReturnNewConfig() {
        CacheConfig<String, String> actualConfig = createCacheConfig(client, newCacheConfig, false);

        assertNotEquals(cacheConfig, actualConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateCacheConfig_rethrowsExceptions() {
        createCacheConfig(exceptionThrowingClient, newCacheConfig, false);
    }

    @Test
    public void testEnableStatisticManagementOnNodes() {
        enableStatisticManagementOnNodes(client, CACHE_NAME, false, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnableStatisticManagementOnNodes_sneakyThrowsException() {
        Member member = mock(Member.class);
        when(member.getUuid()).thenThrow(new IllegalArgumentException("expected"));

        Collection<Member> members = singletonList(member);
        when(exceptionThrowingClient.getClientClusterService().getMemberList()).thenReturn(members);

        enableStatisticManagementOnNodes(exceptionThrowingClient, CACHE_NAME, false, false);
    }
}
