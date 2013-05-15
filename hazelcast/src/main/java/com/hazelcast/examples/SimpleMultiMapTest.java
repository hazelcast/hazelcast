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

package com.hazelcast.examples;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Partition;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class SimpleMultiMapTest {

    public static final int STATS_SECONDS = 10;
    public static int THREAD_COUNT = 40;
    public static int ENTRY_COUNT = 10 * 1000;
    public static int VALUE_SIZE = 1000;
    //    public static int MULTIMAP_VALUE_COUNT = 1;
    public static int GET_PERCENTAGE = 40;
    public static int PUT_PERCENTAGE = 40;

    static Logger logger = Logger.getLogger(SimpleMapTest.class.getName());
    static HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);

    public static void main(String[] args) {
        boolean load = init(args);
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        final MultiMap<String, byte[]> map = instance.getMultiMap("default");
        final AtomicInteger gets = new AtomicInteger(0);
        final AtomicInteger puts = new AtomicInteger(0);
        final AtomicInteger removes = new AtomicInteger(0);
        load(load, es, map);
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.execute(new Runnable() {
                public void run() {
                    while (true) {
                        int key = (int) (Math.random() * ENTRY_COUNT);
                        int operation = ((int) (Math.random() * 100));
                        if (operation < GET_PERCENTAGE) {
                            map.get(String.valueOf(key));
                            gets.incrementAndGet();
                        } else if (operation < GET_PERCENTAGE + PUT_PERCENTAGE) {
                            map.put(String.valueOf(key), new byte[VALUE_SIZE]);
                            puts.incrementAndGet();
                        } else {
                            map.remove(String.valueOf(key));
                            removes.incrementAndGet();
                        }
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(STATS_SECONDS * 1000);
                        logger.info("cluster size:"
                                + instance.getCluster().getMembers().size());
                        int putCount = puts.getAndSet(0);
                        int getCount = gets.getAndSet(0);
                        int removeCount = removes.getAndSet(0);
                        logger.info("TOTAL:" + (removeCount + putCount + getCount) / STATS_SECONDS);
                        logger.info("PUTS:" + putCount / STATS_SECONDS);
                        logger.info("GEtS:" + getCount / STATS_SECONDS);
                        logger.info("REMOVES:" + removeCount / STATS_SECONDS);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        });
    }

    private static void load(boolean load, ExecutorService es, final MultiMap<String, byte[]> map) {
        if (load) {
            final Member thisMember = instance.getCluster().getLocalMember();
            for (int i = 0; i < ENTRY_COUNT; i++) {
                final String key = String.valueOf(i);
                Partition partition = instance.getPartitionService().getPartition(key);
                if (thisMember.equals(partition.getOwner())) {
                    es.execute(new Runnable() {
                        public void run() {
                            map.put(key, new byte[VALUE_SIZE]);
                        }
                    });
                }
            }
        }
    }

    private static boolean init(String[] args) {
        boolean load = false;
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
                } else if (arg.startsWith("load")) {
                    load = true;
                }
            }
        } else {
            logger.info("Help: sh test.sh t200 v130 p10 g85 ");
            logger.info("    // means 200 threads, value-size 130 bytes, 10% put, 85% get");
            logger.info("");
        }
        logger.info("Starting Test with ");
        logger.info("      Thread Count: " + THREAD_COUNT);
        logger.info("       Entry Count: " + ENTRY_COUNT);
        logger.info("        Value Size: " + VALUE_SIZE);
        logger.info("    Get Percentage: " + GET_PERCENTAGE);
        logger.info("    Put Percentage: " + PUT_PERCENTAGE);
        logger.info(" Remove Percentage: " + (100 - (PUT_PERCENTAGE + GET_PERCENTAGE)));
        return load;
    }
}
