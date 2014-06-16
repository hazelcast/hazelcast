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

package com.hazelcast.multimap.standalone;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Partition;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Tests for MultiMap
 */
public final class SimpleMultiMapTest {



    private static Logger logger = Logger.getLogger(SimpleMultiMapTest.class.getName());
    private static HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
    private static final int STATS_SECONDS = 10;
    private static int threadCount = 40;
    private static int entryCount = 10 * 1000;
    private static int valueSize = 1000;
    private static int getPercentage = 40;
    private static int putPercentage = 40;
    private static final Random RANDOM = new Random();

    private SimpleMultiMapTest() {
    }

    public static void main(String[] args) {
        boolean load = init(args);
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        final MultiMap<String, byte[]> map = instance.getMultiMap("default");
        final AtomicInteger gets = new AtomicInteger(0);
        final AtomicInteger puts = new AtomicInteger(0);
        final AtomicInteger removes = new AtomicInteger(0);
        load(load, es, map);
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                public void run() {
                    while (true) {
                        int key = (int) (RANDOM.nextFloat() * entryCount);
                        int operation = ((int) (RANDOM.nextFloat() * 100));
                        if (operation < getPercentage) {
                            map.get(String.valueOf(key));
                            gets.incrementAndGet();
                        } else if (operation < getPercentage + putPercentage) {
                            map.put(String.valueOf(key), new byte[valueSize]);
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
            for (int i = 0; i < entryCount; i++) {
                final String key = String.valueOf(i);
                Partition partition = instance.getPartitionService().getPartition(key);
                if (thisMember.equals(partition.getOwner())) {
                    es.execute(new Runnable() {
                        public void run() {
                            map.put(key, new byte[valueSize]);
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
                    threadCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("c")) {
                    entryCount = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("v")) {
                    valueSize = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("g")) {
                    getPercentage = Integer.parseInt(arg.substring(1));
                } else if (arg.startsWith("p")) {
                    putPercentage = Integer.parseInt(arg.substring(1));
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
        logger.info("      Thread Count: " + threadCount);
        logger.info("       Entry Count: " + entryCount);
        logger.info("        Value Size: " + valueSize);
        logger.info("    Get Percentage: " + getPercentage);
        logger.info("    Put Percentage: " + putPercentage);
        logger.info(" Remove Percentage: " + (100 - (putPercentage + getPercentage)));
        return load;
    }
}
