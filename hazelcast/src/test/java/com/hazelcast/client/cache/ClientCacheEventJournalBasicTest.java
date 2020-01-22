/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.journal.CacheEventJournalBasicTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheEventJournalBasicTest extends CacheEventJournalBasicTest {

    private TestHazelcastFactory factory;
    private HazelcastInstance client;

    @Override
    protected HazelcastInstance getRandomInstance() {
        return client;
    }

    @Override
    protected HazelcastInstance[] createInstances() {
        factory = new TestHazelcastFactory();
        final HazelcastInstance[] instances = factory.newInstances(getConfig(), 2);
        client = factory.newHazelcastClient();
        return instances;
    }

    @Override
    protected CacheManager createCacheManager() {
        final HazelcastClientCachingProvider provider = createClientCachingProvider(client);
        return provider.getCacheManager();
    }

    @After
    public final void terminate() {
        if (factory != null) {
            factory.terminateAll();
        }
        HazelcastClient.shutdownAll();
    }
}
