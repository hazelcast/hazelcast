/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.standalone;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleReplicatedMapTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats stats = new Stats();
    private final Random random;

    private final int threadCount;
    private final int entryCount;
    private final int valueSize;
    private final int getPercentage;
    private final int putPercentage;
    private final boolean load;

    private SimpleReplicatedMapTest(final int threadCount, final int entryCount, final int valueSize,
                                    final int getPercentage, final int putPercentage, final boolean load) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.valueSize = valueSize;
        this.getPercentage = getPercentage;
        this.putPercentage = putPercentage;
        this.load = load;
        Config cfg = new XmlConfigBuilder().build()
                                           .setProperty(ClusterProperty.HEALTH_MONITORING_LEVEL.getName(), "NOISY")
                                           .setProperty(ClusterProperty.HEALTH_MONITORING_DELAY_SECONDS.getName(), "5")
                                           .setProperty(ClusterProperty.PHONE_HOME_ENABLED.getName(), "false")
                                           .setProperty(ClusterProperty.PREFER_IPv4_STACK.getName(), "false");
        instance = Hazelcast.newHazelcastInstance(cfg);
        logger = instance.getLoggingService().getLogger("SimpleReplicatedMapTest");
        random = new Random();
    }

    /**
     * Expects the Management Center to be running.
     */
    public static void main(String[] input) throws Exception {
        int threadCount = 40;
        int entryCount = 300;
        int valueSize = 100;
        int getPercentage = 10;
        int putPercentage = 40;
        boolean load = false;

        if (input != null && input.length > 0) {
            for (String arg : input) {
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
            System.out.println("Help: sh test.sh t200 v130 p10 g85 ");
            System.out.println("means 200 threads, value-size 130 bytes, 10% put, 85% get");
            System.out.println();
        }

        SimpleReplicatedMapTest test
                = new SimpleReplicatedMapTest(threadCount, entryCount, valueSize, getPercentage, putPercentage, load);
        test.start();
    }

    private void start() throws Exception {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
        load(es);
        run(es);
    }

    private void run(ExecutorService es) {
        final ReplicatedMap<String, Object> map = instance.getReplicatedMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            int key = (int) (random.nextFloat() * entryCount);
                            int operation = ((int) (random.nextFloat() * 100));
                            if (operation < getPercentage) {
                                map.get(String.valueOf(key));
                                stats.gets.incrementAndGet();
                            } else if (operation < getPercentage + putPercentage) {
                                map.put(String.valueOf(key), createValue());
                                stats.puts.incrementAndGet();
                            } else {
                                map.remove(String.valueOf(key));
                                stats.removes.incrementAndGet();
                            }
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private Object createValue() {
        return new byte[valueSize];
    }

    private void load(ExecutorService es) throws Exception {
        if (!load) {
            return;
        }

        final ReplicatedMap<String, Object> map = instance.getReplicatedMap(NAMESPACE);
        final Member thisMember = instance.getCluster().getLocalMember();
        List<String> lsOwnedEntries = new LinkedList<String>();
        for (int i = 0; i < entryCount; i++) {
            final String key = String.valueOf(i);
            Partition partition = instance.getPartitionService().getPartition(key);
            if (thisMember.equals(partition.getOwner())) {
                lsOwnedEntries.add(key);
            }
        }
        final CountDownLatch latch = new CountDownLatch(lsOwnedEntries.size());
        for (final String ownedKey : lsOwnedEntries) {
            es.execute(new Runnable() {
                public void run() {
                    map.put(ownedKey, createValue());
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    private void startPrintStats() {
        Thread t = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        stats.printAndReset();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        };
        t.start();
    }

    /**
     * A basic statistics class.
     */
    private class Stats {

        private AtomicLong gets = new AtomicLong();
        private AtomicLong puts = new AtomicLong();
        private AtomicLong removes = new AtomicLong();

        void printAndReset() {
            long getsNow = gets.getAndSet(0);
            long putsNow = puts.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            long total = getsNow + putsNow + removesNow;

            logger.info("total= " + total + ", gets:" + getsNow
                    + ", puts:" + putsNow + ", removes:" + removesNow);
            logger.info("Operations per Second: " + total / STATS_SECONDS);
        }
    }

    private void printVariables() {
        logger.info("Starting Test with ");
        logger.info("Thread Count: " + threadCount);
        logger.info("Entry Count: " + entryCount);
        logger.info("Value Size: " + valueSize);
        logger.info("Get Percentage: " + getPercentage);
        logger.info("Put Percentage: " + putPercentage);
        logger.info("Remove Percentage: " + (100 - (putPercentage + getPercentage)));
        logger.info("Load: " + load);
    }
}
