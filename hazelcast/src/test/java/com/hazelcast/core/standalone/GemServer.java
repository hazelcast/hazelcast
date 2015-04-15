package com.hazelcast.core.standalone;/*
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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Client;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author mdogan 05/03/15
 */
public class GemServer {

    public static void main(String[] args) throws Exception {
        final HazelcastInstance hz = build();
        final IMap<Object, Object> map = hz.getMap("test");
        map.size();

        int threads = Runtime.getRuntime().availableProcessors();
        final ScheduledExecutorService ex = Executors.newScheduledThreadPool(threads);
        final Thread[] threadArray = new Thread[threads];

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                ex.shutdownNow();
                hz.getLifecycleService().terminate();

                for (Thread thread : threadArray) {
                    thread.interrupt();
                }
            }
        });

        final AtomicLong deleted = addEntryListener(map, ex);

        final AtomicLongArray timestamps = new AtomicLongArray(threads);
        for (int i = 0; i < threads; i++) {
            final int ix = i;
            Thread thread = new Thread() {
                public void run() {
                    Random rand = new Random();
                    timestamps.set(ix, System.currentTimeMillis());
                    ILogger logger = hz.getLoggingService().getLogger(GemServer.class);
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
        long timeout = TimeUnit.SECONDS.toMillis(61);

        Thread sizeThread = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    try {
                        System.out.println("MAP: " + map.size());
                        System.out.println("DELETED: " + deleted.get());
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
                    System.out.println(
                            thread.getName() + " is stuck for " + TimeUnit.MILLISECONDS.toMinutes(now - ts) + " mins!");
                }
            }

            Set<Member> members = hz.getCluster().getMembers();
            System.out.println("CLUSTER[" + members.size() + "]:");
            for (Member member : members) {
                System.out.println("\t" + member);
            }
            System.out.println();

            Collection<Client> clients = hz.getClientService().getConnectedClients();
            System.out.println("CLIENTS[" + clients.size() + "]:");
            for (Client client : clients) {
                System.out.println("\t" + client);
            }

            System.out.println();
            System.out.println();
            Thread.sleep(10000);
        }
    }

    private static AtomicLong addEntryListener(final IMap<Object, Object> map, final ScheduledExecutorService ex) {
        final AtomicLong deleted = new AtomicLong();
        map.addLocalEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(final EntryEvent<Object, Object> event) {
                final Object key = event.getKey();
                ex.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            map.delete(key);
                            deleted.incrementAndGet();
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }
                    }
                }, 10, TimeUnit.SECONDS);
            }

            @Override
            public void entryUpdated(final EntryEvent<Object, Object> event) {
                final Object key = event.getKey();
                ex.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            map.delete(key);
                            deleted.incrementAndGet();
                        } catch (Exception e) {
                            System.err.println(e.getMessage());
                        }

                    }
                }, 10, TimeUnit.SECONDS);
            }
        });
        return deleted;
    }

    private static HazelcastInstance build() throws Exception {
        Config config = new XmlConfigBuilder().build();
        config.setProperty(GroupProperties.PROP_SHUTDOWNHOOK_ENABLED, "false");

        return Hazelcast.newHazelcastInstance(config);
    }
}
