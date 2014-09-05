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

package com.hazelcast.map.standalone;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple functional Map test
 */
public final class SimpleFunctionalMapTest {

    private static final int ENTRY_COUNT = 1000;
    private static final int KB = 10240;
    private static final int STATS_SECONDS = 10;
    private static final Random RANDOM = new Random();

    private SimpleFunctionalMapTest() {
    }

    /**
     * This test runs continuously until an exception is thrown.
     * No args
     */
    public static void main(String[] args) {

        int threadCount = 40;
        final Stats stats = new Stats();
        final HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            es.submit(new Runnable() {
                public void run() {
                    IMap map = hazelcast.getMap("default");
                    while (true) {
                        int keyInt = (int) (RANDOM.nextFloat() * ENTRY_COUNT);
                        int operation = ((int) (RANDOM.nextFloat() * 1000)) % 20;
                        Object key = String.valueOf(keyInt);
                        if (operation < 1) {
                            map.size();
                            stats.increment("size");
                        } else if (operation < 2) {
                            map.get(key);
                            stats.increment("get");
                        } else if (operation < 3) {
                            map.remove(key);
                            stats.increment("remove");
                        } else if (operation < 4) {
                            map.containsKey(key);
                            stats.increment("containsKey");
                        } else if (operation < 5) {
                            Object value = String.valueOf(key);
                            map.containsValue(value);
                            stats.increment("containsValue");
                        } else if (operation < 6) {
                            map.putIfAbsent(key, createValue());
                            stats.increment("putIfAbsent");
                        } else if (operation < 7) {
                            Collection col = map.values();
                            for (Object o : col) {
                                int i = 0;
                            }
                            stats.increment("values");
                        } else if (operation < 8) {
                            Collection col = map.keySet();
                            for (Object o : col) {
                                int i = 0;
                            }
                            stats.increment("keySet");
                        } else if (operation < 9) {
                            Collection col = map.entrySet();
                            for (Object o : col) {
                                int i = 0;
                            }
                            stats.increment("entrySet");
                        } else {
                            map.put(key, createValue());
                            stats.increment("put");
                        }
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("cluster size:"
                                + hazelcast.getCluster().getMembers().size());
                        Stats currentStats = stats.getAndReset();
                        System.out.println(currentStats);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public static Object createValue() {
        int numberOfK = (((int) (RANDOM.nextFloat() * 1000)) % 40) + 1;
        return new byte[numberOfK * KB];
    }

    /**
     * Map statistics class
     */
    public static class Stats {
        Map<String, AtomicLong> mapStats = new ConcurrentHashMap<String, AtomicLong>(10);

        public Stats() {
            mapStats.put("put", new AtomicLong(0));
            mapStats.put("get", new AtomicLong(0));
            mapStats.put("remove", new AtomicLong(0));
            mapStats.put("size", new AtomicLong(0));
            mapStats.put("containsKey", new AtomicLong(0));
            mapStats.put("containsValue", new AtomicLong(0));
            mapStats.put("clear", new AtomicLong(0));
            mapStats.put("keySet", new AtomicLong(0));
            mapStats.put("values", new AtomicLong(0));
            mapStats.put("entrySet", new AtomicLong(0));
            mapStats.put("putIfAbsent", new AtomicLong(0));
        }

        public Stats getAndReset() {
            Stats newOne = new Stats();
            Set<Map.Entry<String, AtomicLong>> entries = newOne.mapStats.entrySet();
            for (Map.Entry<String, AtomicLong> entry : entries) {
                String key = entry.getKey();
                AtomicLong value = entry.getValue();
                value.set(mapStats.get(key).getAndSet(0));
            }
            return newOne;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            long total = 0;
            Set<Map.Entry<String, AtomicLong>> entries = mapStats.entrySet();
            for (Map.Entry<String, AtomicLong> entry : entries) {
                String key = entry.getKey();
                AtomicLong value = entry.getValue();
                sb.append(key + ":" + value.get());
                sb.append("\n");
                total += value.get();
            }
            sb.append("Operations per Second : " + total / STATS_SECONDS + " \n");
            return sb.toString();
        }

        public void increment(String operation) {
            mapStats.get(operation).incrementAndGet();
        }
    }
}
