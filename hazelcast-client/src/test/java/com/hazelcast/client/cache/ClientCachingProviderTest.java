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

package com.hazelcast.client.cache;

import com.hazelcast.cache.CachingProviderTest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientCachingProviderTest extends CachingProviderTest {

    private final List<HazelcastInstance> instances = new ArrayList<HazelcastInstance>();

    @Override
    protected TestHazelcastInstanceFactory createTestHazelcastInstanceFactory(int count) {
        // Since `HazelcastClient.getHazelcastClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        instances.add(instance);
        // Since we are using real instances, no need to mock instance factory.
        return null;
    }

    @Override
    protected HazelcastInstance createCacheInstance() {
        // Since `HazelcastClient.getHazelcastClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        instances.add(instance);
        return instance;
    }

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance defaultInstance) {
        return HazelcastClientCachingProvider.createCachingProvider(defaultInstance);
    }

    @After
    public void tearDown() {
        Iterator<HazelcastInstance> iter = instances.iterator();
        while (iter.hasNext()) {
            HazelcastInstance instance = iter.next();
            try {
                instance.shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                iter.remove();
            }
        }
    }

}
