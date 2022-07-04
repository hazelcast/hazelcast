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

package com.hazelcast.client.starter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastClientStarterTest {

    private HazelcastInstance memberInstance;

    @After
    public void tearDown() {
        if (memberInstance != null) {
            memberInstance.shutdown();
        }
    }

    @Test
    public void testClientLifecycle() {
        memberInstance = HazelcastStarter.newHazelcastInstance("4.0.3");

        for (int i = 1; i < 4; i++) {
            String version = "4.0." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastClientStarter.newHazelcastClient(version, false);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }
    }

    @Test
    public void testClientMap() {
        HazelcastInstance clientInstance = null;
        try {
            memberInstance = HazelcastStarter.newHazelcastInstance("4.0.3");
            clientInstance = HazelcastClientStarter.newHazelcastClient("4.0.3", false);

            IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
            IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

            clientMap.put(1, 2);

            assertEquals(2, (int) memberMap.get(1));
        } finally {
            if (clientInstance != null) {
                clientInstance.shutdown();
            }
        }
    }

    @Test
    public void testAdvancedClientMap() {
        memberInstance = HazelcastStarter.newHazelcastInstance("4.0.3");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("4.0.3", false);

        System.out.println("About to terminate the client");
        clientInstance.getLifecycleService().terminate();
        System.out.println("Client terminated");
    }
}
