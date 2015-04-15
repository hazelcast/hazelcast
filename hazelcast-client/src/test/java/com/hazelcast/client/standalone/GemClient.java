/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.standalone;/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author mdogan 05/03/15
 */
public class GemClient {

    public static void main(String[] args) throws Exception {
        ClientConfig config = new XmlClientConfigBuilder().build();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        final IMap<Object, Object> map = hz.getMap("test");
        map.size();

        int threads = Runtime.getRuntime().availableProcessors();
        final Thread[] threadArray = new Thread[threads];

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                hz.getLifecycleService().terminate();

                for (Thread thread : threadArray) {
                    thread.interrupt();
                }
            }
        });

        final AtomicLongArray timestamps = new AtomicLongArray(threads);
        for (int i = 0; i < threads; i++) {
            final int ix = i;
            Thread thread = new Thread() {
                public void run() {
                    Random rand = new Random();
                    timestamps.set(ix, System.currentTimeMillis());
                    ILogger logger = Logger.getLogger(GemClient.class);
                    while (!isInterrupted()) {
                        String key = "Z" + rand.nextInt(100) + "X";
                        ILock lock = hz.getLock(key);
                        try {
                            lock.lock();
                            try {
                                long now = System.currentTimeMillis();
                                timestamps.set(ix, now);
                                map.put(rand.nextInt(100000), now);
                            } finally {
                                lock.unlock();
                            }
                        } catch (Exception e) {
                            String message = e.getClass().getName() + ": " + e.getMessage();
                            if (!(e instanceof HazelcastInstanceNotActiveException)) {
                                logger.warning(message);
                            }
                        }

                        try {
                            sleep(1);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                }
            };
            threadArray[ix] = thread;
            thread.start();
        }

        Thread main = Thread.currentThread();
        long timeout = TimeUnit.SECONDS.toMillis(30);

        Thread sizeThread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        System.out.println("MAP: " + map.size());
                    } catch (Exception e) {
                        System.err.println("error during size: " + e);
                    }
                    try {
                        sleep(10000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        };
        sizeThread.setDaemon(true);
        sizeThread.start();

        while (!main.isInterrupted()) {
            long now = System.currentTimeMillis();
            for (int i = 0; i < threads; i++) {
                long ts = timestamps.get(i);
                if (ts + timeout < now) {
                    Thread thread = threadArray[i];
                    System.out.println(thread.getName() + " is stuck for "
                            + TimeUnit.MILLISECONDS.toMinutes(now - ts) + " mins!");
                }
            }

            Set<Member> members = hz.getCluster().getMembers();
            System.out.println("CLUSTER["  + members.size() + "]: " + members);
            System.out.println("THIS: " + hz.getLocalEndpoint());
            System.out.println();
            Thread.sleep(10000);
        }
    }
}
