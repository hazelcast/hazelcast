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

package com.hazelcast.cache;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple test of a map.
 */
public final class SimpleConcurrentMapTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

//    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats stats = new Stats();
    private final Random random;

    private final int threadCount;
    private final int entryCount;
    private final int valueSize;
    private final int getPercentage;
    private final int putPercentage;
    private final boolean load;
    final SerializationService ss = new SerializationServiceBuilder().build();

    private SimpleConcurrentMapTest(final int threadCount, final int entryCount, final int valueSize,
                                    final int getPercentage, final int putPercentage, final boolean load) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.valueSize = valueSize;
        this.getPercentage = getPercentage;
        this.putPercentage = putPercentage;
        this.load = load;
//        Config cfg = new XmlConfigBuilder().build();
//
//        instance = Hazelcast.newHazelcastInstance(cfg);
////        Hazelcast.newHazelcastInstance(cfg);
//
        logger = Logger.getLogger("SM");
        random = new Random();
    }

    /**
     *
     * Expects the Management Center to be running.
     * @param input
     * @throws InterruptedException
     */
    public static void main(String[] input) throws InterruptedException {
        int threadCount = 40;
        int entryCount = 10 * 1000;
        int valueSize = 1000;
        int getPercentage = 40;
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

        SimpleConcurrentMapTest test = new SimpleConcurrentMapTest(threadCount, entryCount, valueSize, getPercentage, putPercentage, load);
        test.start();
    }

    private void start() throws InterruptedException {
        printVariables();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        startPrintStats();
        load(es);
        run(es);
    }

    private void run(ExecutorService es) {
//        HazelcastCachingProvider hcp = new HazelcastCachingProvider();
//
//        HazelcastCacheManager cacheManager = new HazelcastCacheManager(hcp,instance,hcp.getDefaultURI(),hcp.getDefaultClassLoader(),null);

//        final CachingProvider cachingProvider = Caching.getCachingProvider();
//
//        final CacheManager cacheManager = cachingProvider.getCacheManager();
//

//        CacheConfig<String, Object> config = new CacheConfig<String, Object>();

//        final Cache<String, Object> cache = cacheManager.createCache(NAMESPACE, config);


//        Data data = ss.toData(value);

        final ConcurrentReferenceHashMap<Data, Data> cache = new ConcurrentReferenceHashMap<Data, Data>(100,0.91f,40,
                ConcurrentReferenceHashMap.ReferenceType.STRONG, ConcurrentReferenceHashMap.ReferenceType.STRONG,null);
//        final IMap<String, Object> map = instance.getMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            int keyValue = (int) (random.nextFloat() * entryCount);
                            Data key = ss.toData(keyValue);
                            int operation = ((int) (random.nextFloat() * 100));
                            if (operation < getPercentage) {
                                final Data data = cache.get(key);
                                ss.toObject(data);
                                stats.gets.incrementAndGet();
                            } else if (operation < getPercentage + putPercentage) {
                                final Data oldData = cache.put(key, createValue());
                                final Object o = ss.toObject(oldData);
                                stats.puts.incrementAndGet();
                            } else {
                                final Data remove = cache.remove(key);
                                final Object o = ss.toObject(remove);
                                stats.removes.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private Data createValue() {
        return ss.toData(new byte[valueSize]);
    }

    private void load(ExecutorService es) throws InterruptedException {
//        if (!load) {
//            return;
//        }
//
//        final IMap<String, Object> map = instance.getMap(NAMESPACE);
//        final Member thisMember = instance.getCluster().getLocalMember();
//        List<String> lsOwnedEntries = new LinkedList<String>();
//        for (int i = 0; i < entryCount; i++) {
//            final String key = String.valueOf(i);
//            Partition partition = instance.getPartitionService().getPartition(key);
//            if (thisMember.equals(partition.getOwner())) {
//                lsOwnedEntries.add(key);
//            }
//        }
//        final CountDownLatch latch = new CountDownLatch(lsOwnedEntries.size());
//        for (final String ownedKey : lsOwnedEntries) {
//            es.execute(new Runnable() {
//                public void run() {
//                    map.put(ownedKey, createValue());
//                    latch.countDown();
//                }
//            });
//        }
//        latch.await();
    }

    private void startPrintStats() {
        Thread t = new Thread() {
            {
                setDaemon(true);
                setName("PrintStats.");
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
     * A basic statistics class
     */
    private class Stats {

        private AtomicLong gets = new AtomicLong();
        private AtomicLong puts = new AtomicLong();
        private AtomicLong removes = new AtomicLong();

        public void printAndReset() {
            long getsNow = gets.getAndSet(0);
            long putsNow = puts.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            long total = getsNow + putsNow + removesNow;

            logger.info("total= " + total + ", gets:" + getsNow
                    + ", puts:" + putsNow + ", removes:" + removesNow);
            logger.info("Operations per Second : " + total / STATS_SECONDS);
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
