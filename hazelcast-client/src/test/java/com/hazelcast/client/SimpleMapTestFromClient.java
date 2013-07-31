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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Ignore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("not a JUnit test")
public class SimpleMapTestFromClient {

    public static int THREAD_COUNT = 4;
    public static int ENTRY_COUNT = 10 * 1000;
    public static int VALUE_SIZE = 1000;
    public static final int STATS_SECONDS = 10;
    public static int GET_PERCENTAGE = 40;
    public static int PUT_PERCENTAGE = 40;

    public static void main(String[] args) {
        final ClientConfig clientConfig = new ClientConfig();
//        clientConfig.addNearCacheConfig("*", new NearCacheConfig().setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT).setInvalidateOnChange(false));
        final HazelcastInstance hazelcast = HazelcastClient.newHazelcastClient(clientConfig);
        final Stats stats = new Stats();
        if (args != null && args.length > 0) {
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("t")) {
                    THREAD_COUNT = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    ENTRY_COUNT = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    VALUE_SIZE = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("g")) {
                    GET_PERCENTAGE = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    PUT_PERCENTAGE = Integer.parseInt(arg.substring(1));
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
            System.out.println("    // means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println("");
        }
        System.out.println("Starting Test with ");
        System.out.println("      Thread Count: " + THREAD_COUNT);
        System.out.println("       Entry Count: " + ENTRY_COUNT);
        System.out.println("        Value Size: " + VALUE_SIZE);
        System.out.println("    Get Percentage: " + GET_PERCENTAGE);
        System.out.println("    Put Percentage: " + PUT_PERCENTAGE);
        System.out.println(" Remove Percentage: " + (100 - (PUT_PERCENTAGE + GET_PERCENTAGE)));
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.submit(new Runnable() {
                public void run() {
                    IMap<String, byte[]> map = hazelcast.getMap("default");
                    while (true) {
                        int key = (int) (Math.random() * ENTRY_COUNT);
                        int operation = ((int) (Math.random() * 100));
                        if (operation < GET_PERCENTAGE) {
//                            long start = Clock.currentTimeMillis();
                            map.get(String.valueOf(key));
//                            System.out.println("Get takes " + (Clock.currentTimeMillis() - start) + " ms" );
                            stats.gets.incrementAndGet();
                        } else if (operation < GET_PERCENTAGE + PUT_PERCENTAGE) {
                            map.put(String.valueOf(key), new byte[VALUE_SIZE]);
                            stats.puts.incrementAndGet();
                        } else {
                            map.remove(String.valueOf(key));
                            stats.removes.incrementAndGet();
                        }
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("cluster size:"
                                + hazelcast.getCluster().getMembers().size());
                        Stats currentStats = stats.getAndReset();
                        System.out.println(currentStats);
                        System.out.println("Operations per Second : " + currentStats.total()
                                / STATS_SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public static class Stats {
        public AtomicLong puts = new AtomicLong();
        public AtomicLong gets = new AtomicLong();
        public AtomicLong removes = new AtomicLong();

        public Stats getAndReset() {
            long putsNow = puts.getAndSet(0);
            long getsNow = gets.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            Stats newOne = new Stats();
            newOne.puts.set(putsNow);
            newOne.gets.set(getsNow);
            newOne.removes.set(removesNow);
            return newOne;
        }

        public long total() {
            return puts.get() + gets.get() + removes.get();
        }

        public String toString() {
            return "total= " + total() + ", gets:" + gets.get() + ", puts: " + puts.get() + ", removes:" + removes.get();
        }
    }
}
