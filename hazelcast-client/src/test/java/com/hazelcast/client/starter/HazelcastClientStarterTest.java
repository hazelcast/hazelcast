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

package com.hazelcast.client.starter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastClientStarterTest {

    @Test
    public void testClientLifecycle() {
        HazelcastInstance member = HazelcastStarter.newHazelcastInstance("3.7");

        for (int i = 1; i < 6; i++) {
            String version = "3.7." + i;
            System.out.println("Starting client " + version);
            HazelcastInstance instance = HazelcastClientStarter.newHazelcastClient(version, false);
            System.out.println("Stopping client " + version);
            instance.shutdown();
        }

        member.shutdown();
    }

    @Test
    public void testClientMap() {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        IMap<Integer, Integer> memberMap = memberInstance.getMap("myMap");

        clientMap.put(1, 2);

        assertEquals(2, (int) memberMap.get(1));

        clientInstance.shutdown();
        memberInstance.shutdown();
    }

    @Test
    public void testAdvancedClientMap() {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        System.out.println("About to terminate the client");
        clientInstance.getLifecycleService().terminate();
        System.out.println("Client terminated");

        memberInstance.shutdown();
    }

    @Test
    public void testClientMap_async() throws InterruptedException, ExecutionException {
        HazelcastInstance memberInstance = HazelcastStarter.newHazelcastInstance("3.7");
        HazelcastInstance clientInstance = HazelcastClientStarter.newHazelcastClient("3.7.2", false);

        IMap<Integer, Integer> clientMap = clientInstance.getMap("myMap");
        clientMap.put(0, 1);
        ICompletableFuture<Integer> async = clientMap.getAsync(0);
        int value = async.get();

        assertEquals(1, value);

        clientInstance.shutdown();
        memberInstance.shutdown();
    }
}
