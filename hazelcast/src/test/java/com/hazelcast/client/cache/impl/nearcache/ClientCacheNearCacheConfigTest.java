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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.client.config.XmlClientConfigBuilderTest.buildConfig;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static java.lang.Integer.MAX_VALUE;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheConfigTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setUp() throws Exception {
        factory.newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void native_backed_nearCache_throws_illegalArgumentException_whenNoNativeConfigAvailable() throws Exception {
        String xml = "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">"
                + "<near-cache name=\"test\">"
                + "<in-memory-format>NATIVE</in-memory-format>"
                + "</near-cache></hazelcast-client>";

        ClientConfig clientConfig = buildConfig(xml);
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        cacheManager.createCache("test", newCacheConfig());
    }

    private CacheConfig newCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.getEvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return cacheConfig;
    }
}
