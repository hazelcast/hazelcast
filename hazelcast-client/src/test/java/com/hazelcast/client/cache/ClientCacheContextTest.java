/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheContextTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientCacheContextTest extends CacheContextTest {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    @Override
    public void setup() {
        hazelcastInstance1 = factory.newHazelcastInstance();
        hazelcastInstance2 = factory.newHazelcastInstance();

        driverInstance = factory.newHazelcastClient();
        provider = HazelcastClientCachingProvider.createCachingProvider(driverInstance);
    }

    @After
    public void tearDown() {
        HazelcastClient.shutdownAll();
        driverInstance.shutdown();
        hazelcastInstance1.shutdown();
        hazelcastInstance2.shutdown();
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterDeregister() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.DEREGISTER);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterShutdown() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.SHUTDOWN);
    }

    @Test
    public void cacheEntryListenerCountIncreasedAfterRegisterAndDecreasedAfterTerminate() {
        cacheEntryListenerCountIncreasedAndDecreasedCorrectly(DecreaseType.TERMINATE);
    }
}
