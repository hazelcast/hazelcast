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

package com.hazelcast.client.cache.eviction;

import com.hazelcast.cache.eviction.CacheEvictionPolicyComparatorTest;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheEvictionPolicyComparatorTest extends CacheEvictionPolicyComparatorTest {

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance instance;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createClientCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        instance = instanceFactory.newHazelcastInstance(config);
        return instanceFactory.newHazelcastClient();
    }

    @Override
    protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance) {
        return instance.getUserContext();
    }

    @After
    public void tearDown() {
        instanceFactory.shutdownAll();
    }

}
