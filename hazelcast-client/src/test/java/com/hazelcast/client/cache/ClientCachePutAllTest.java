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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CachePutAllTest;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCachePutAllTest extends CachePutAllTest {

    private TestHazelcastFactory clientFactory;
    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        clientFactory = new TestHazelcastFactory();
        return clientFactory;
    }

    protected ClientConfig createClientConfig() {
        return new ClientConfig();
    }

    @Override
    protected void onSetup() {
        super.onSetup();
        ClientConfig clientConfig = createClientConfig();
        client = clientFactory.newHazelcastClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        // Client factory is already shutdown at this test's super class (`CachePutAllTest`)
        // because it is returned instance factory from overridden `getInstanceFactory` method.
        client = null;
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return HazelcastClientCachingProvider.createCachingProvider(client);
    }

}
