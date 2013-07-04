/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.*;

/**
 * @ali 7/3/13
 */
public class ClientIssueTest {


    public static void main(String[] args) throws Exception {
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        final HazelcastInstance c = HazelcastClient.newHazelcastClient(new ClientConfig());

        final Thread thread = new Thread() {
            public void run() {
                final IMap<Object, Object> map = c.getMap("test");
                map.addEntryListener(new EntryAdapter<Object, Object>() {
                    public void entryAdded(EntryEvent<Object, Object> event) {
                        System.err.println("event = " + event);
                    }
                }, true);

                for (int i = 0; i < 40; i++) {
                    try {
                        map.put(System.currentTimeMillis(), Math.random());
                        System.err.println("put");
                        sleep(500);
                    } catch (Exception e) {
                        System.err.println("-------");
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();

        Thread.sleep(5000);

        hz1.getLifecycleService().terminate();
        thread.join();
        c.getLifecycleService().terminate();
        hz2.getLifecycleService().terminate();
    }
}
