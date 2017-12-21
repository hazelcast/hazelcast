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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.TestCase.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class ClientMapNearCachePreloaderStressTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testDestroyAndCreateProxyWithNearCache() {
        factory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(getNearCacheConfig("test"));
        final HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        int createPutGetThreadCount = 2;
        int destroyThreadCount = 2;
        ExecutorService pool = Executors.newFixedThreadPool(createPutGetThreadCount + destroyThreadCount);

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        final AtomicReference<Exception> exception = new AtomicReference<Exception>();
        for (int i = 0; i < destroyThreadCount; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    while (isRunning.get()) {
                        for (DistributedObject distributedObject : client.getDistributedObjects())
                            distributedObject.destroy();
                    }
                }
            });
        }

        for (int i = 0; i < createPutGetThreadCount; i++) {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (isRunning.get()) {
                            IMap<Object, Object> map = client.getMap("test");
                            map.put(1, 1);
                            map.get(1);
                        }
                    } catch (Exception e) {
                        isRunning.set(false);
                        e.printStackTrace(System.out);
                        exception.set(e);
                    }
                }
            });
        }

        sleepSeconds(5);
        isRunning.set(false);
        pool.shutdown();
        assertNull(exception.get());
    }

    private NearCacheConfig getNearCacheConfig(String name) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        NearCachePreloaderConfig preloaderConfig = new NearCachePreloaderConfig();
        preloaderConfig.setStoreIntervalSeconds(1);
        preloaderConfig.setEnabled(true);
        nearCacheConfig.setPreloaderConfig(preloaderConfig);
        nearCacheConfig.setName(name);
        return nearCacheConfig;
    }
}
