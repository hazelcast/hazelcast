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

package com.hazelcast.client.io;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientExecutionPoolSizeLowTest {

    static final int COUNT = 1000;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance server1;
    private HazelcastInstance server2;
    private IMap map;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }


    @Before
    public void setup() throws IOException {
        server1 = hazelcastFactory.newHazelcastInstance();
        server2 = hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setExecutorPoolSize(1);
        clientConfig.getNetworkConfig().setRedoOperation(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        map = client.getMap(randomString());
    }

    @Test
    public void testNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test
    public void testOwnerNodeTerminate() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.put(i, i);
            if (i == COUNT / 2) {
                server1.getLifecycleService().terminate();
            }
        }
        assertEquals(COUNT, map.size());
    }

    @Test
    public void testNodeTerminateWithAsyncOperations() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);
            if (i == COUNT / 2) {
                server2.getLifecycleService().terminate();
            }
        }
        assertSizeEventually(COUNT, map);
    }

    @Test
    public void testOwnerNodeTerminateWithAsyncOperations() throws InterruptedException, ExecutionException {
        for (int i = 0; i < COUNT; i++) {
            map.putAsync(i, i);

            if (i == COUNT / 2) {
                server1.getLifecycleService().terminate();
            }
        }

        assertSizeEventually(COUNT, map);
    }
}
