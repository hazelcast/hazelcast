/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientEntryListenerDisconnectTest {

    private static final AtomicInteger ENTRY_ADDED = new AtomicInteger(0);
    private static final AtomicInteger ENTRY_EVICTED = new AtomicInteger(0);

    private ClientEntryListenerDisconnectTest() {
    }

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setGroupConfig(new GroupConfig("test", "test"));
        config.getNetworkConfig().setPort(6701);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        IMap<Integer, GenericEvent> map = server.getMap("test");
        map.addIndex("userId", false);

        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("localhost:6701", "localhost:6702");
        clientConfig.setGroupConfig(new GroupConfig("test", "test"));
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(100);
        clientConfig.getNetworkConfig().setSmartRouting(false);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Integer, GenericEvent> mapClient = client.getMap("test");

        mapClient.addEntryListener(new EntryAdapter<Integer, GenericEvent>() {
            public void entryAdded(EntryEvent<Integer, GenericEvent> event) {
                ENTRY_ADDED.incrementAndGet();
            }

            public void entryEvicted(EntryEvent<Integer, GenericEvent> event) {
                if (event.getValue() == null) {
                    ENTRY_EVICTED.incrementAndGet();
                }
            }
        }, true);

        HazelcastInstance client2 = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Integer, GenericEvent> mapClient2 = client2.getMap("test");

        map.put(1, new GenericEvent(1), 5, TimeUnit.SECONDS);
        Thread.sleep(20);
        mapClient.remove(1);

        server.getLifecycleService().terminate();

        Thread.sleep(15000);

        mapClient2.put(2, new GenericEvent(2), 1, TimeUnit.SECONDS);
        Thread.sleep(20);
        mapClient2.remove(2);
        mapClient2.put(3, new GenericEvent(3), 1, TimeUnit.SECONDS);

        Thread.sleep(15000);

        server = Hazelcast.newHazelcastInstance(config);
        map = server.getMap("test");

        map.put(4, new GenericEvent(4), 1, TimeUnit.SECONDS);
        map.put(5, new GenericEvent(5), 5, TimeUnit.SECONDS);
        map.put(6, new GenericEvent(6), 1, TimeUnit.SECONDS);
        map.put(7, new GenericEvent(7), 1, TimeUnit.SECONDS);

        Thread.sleep(10000);

        if (ENTRY_EVICTED.get() != 0) {
            System.out.println("ERROR: got " + ENTRY_EVICTED + " evictions with null values");
        } else {
            System.out.println("OK");
        }

        mapClient.put(8, new GenericEvent(8), 1, TimeUnit.SECONDS);

        Thread.sleep(5000);

        if (ENTRY_ADDED.get() != 8) {
            System.out.println("ERROR: got " + ENTRY_ADDED + " instead of 8");
        } else {
            System.out.println("OK");
        }

        System.exit(0);
    }

    @SuppressWarnings("unused")
    private static class GenericEvent implements Serializable {
        private static final long serialVersionUID = -933111044641052844L;

        private int userId;

        public GenericEvent(int userId) {
            this.setUserId(userId);
        }

        public int getUserId() {
            return userId;
        }

        public void setUserId(int userId) {
            this.userId = userId;
        }
    }
}
