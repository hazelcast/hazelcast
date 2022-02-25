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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.Ignore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Ignore("Not a JUnit test")
public class SimpleMapTestFromClient {

    static {
        ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.setSystemProperty("0");
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        ClusterProperty.PHONE_HOME_ENABLED.setSystemProperty("false");
        ClusterProperty.SOCKET_BIND_ANY.setSystemProperty("false");
    }

    private static int threadCount = 40;
    private static int entryCount = 10 * 1000;
    private static int valueSize = 1000;
    private static int statsSeconds = 10;
    private static int getPercentage = 40;
    private static int putPercentage = 40;

    public static void main(String[] args) {
        final ClientConfig clientConfig = new ClientConfig();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final Stats stats = new Stats();
        if (args != null && args.length > 0) {
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("t")) {
                    threadCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    entryCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    valueSize = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("g")) {
                    getPercentage = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    putPercentage = Integer.parseInt(arg.substring(1));
                }
            }
        } else {
            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
            System.out.println("    // means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println("");
        }
        System.out.println("Starting Test with ");
        System.out.println("      Thread Count: " + threadCount);
        System.out.println("       Entry Count: " + entryCount);
        System.out.println("        Value Size: " + valueSize);
        System.out.println("    Get Percentage: " + getPercentage);
        System.out.println("    Put Percentage: " + putPercentage);
        System.out.println(" Remove Percentage: " + (100 - (putPercentage + getPercentage)));
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            es.submit(new Runnable() {
                public void run() {
                    IMap<String, Object> map = client.getMap("default");
                    while (true) {
                        int key = (int) (Math.random() * entryCount);
                        int operation = ((int) (Math.random() * 100));
                        if (operation < getPercentage) {
                            map.get(String.valueOf(key));
                            stats.gets.incrementAndGet();
                        } else if (operation < getPercentage + putPercentage) {
                            map.put(String.valueOf(key), new byte[valueSize]);
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
                        Thread.sleep(statsSeconds * 1000);
                        System.out.println("cluster size:"
                                + client.getCluster().getMembers().size());
                        Stats currentStats = stats.getAndReset();
                        System.out.println(currentStats);
                        System.out.println("Operations per Second: " + currentStats.total()
                                / statsSeconds);
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
