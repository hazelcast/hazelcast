/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Transaction;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LongRunningTransactionTest {

    private static final int STATS_SECONDS = 10;
    private List<TheNode> nodes = new CopyOnWriteArrayList<TheNode>();
    private int nodeIdGen = 0;
    private final Logger logger = Logger.getLogger(LongRunningTransactionTest.class.getName());
    private int starts, stops, restarts = 0;
    private final AtomicInteger ids = new AtomicInteger();
    private final Timer producer = new Timer();
    private final BlockingQueue<Integer> processedIds = new LinkedBlockingQueue<Integer>();
    private final Random random = new Random();

    public static void main(String[] args) {
        LongRunningTransactionTest t = new LongRunningTransactionTest();
        t.run();
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log("Shutting down " + nodes.size());
                while (nodes.size() > 0) {
                    removeNode();
                }
            }
        });
        while (true) {
            if (nodes.size() > 4) {
                removeNode();
            } else if (nodes.size() == 0) {
                addNode();
                addNode();
                addNode();
                startIdProducer();
                startProcessWatcher();
            } else if (nodes.size() < 2) {
                addNode();
            } else {
                int action = random(3);
                switch (action) {
                    case 0:
                        removeNode();
                        break;
                    case 1:
                        addNode();
                        break;
                    case 2:
                        restartNode();
                        break;
                }
            }
            try {
                int nextSeconds = random(60, 260);
                log("Next Action after " + nextSeconds + " seconds.");
                log("members:" + nodes.size() + ", starts: " + starts + ", stops:" + stops + ", restart:" + restarts);
                log("transaction count: " + ids.get());
                //noinspection BusyWait
                Thread.sleep(nextSeconds * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

    private void startIdProducer() {
        producer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                IQueue<Integer> q = Hazelcast.getQueue("default");
                if (q.size() < 5000) {
                    for (int i = 0; i < 5000; i++) {
                        q.offer(ids.incrementAndGet());
                    }
                }
            }
        }, 0, 1000);
    }

    private void startProcessWatcher() {
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                Set<Integer> outOfOrderIds = new TreeSet<Integer>();
                int lastId = 0;
                while (true) {
                    try {
                        Integer id = processedIds.take();
                        if (id % 5000 == 0) {
                            log("ID " + id + "  mapSize " + Hazelcast.getMap("default").size()
                                    + " OO " + outOfOrderIds.size() + ", lastId:" + lastId);
                        }
                        if (id == (lastId + 1)) {
                            lastId = id;
                            if (outOfOrderIds.size() > 0) {
                                if (outOfOrderIds.size() > 20) {
                                    log("Consuming outOfOrders " + outOfOrderIds.size());
                                }
                                if (outOfOrderIds.size() > 1000) {
                                    //noinspection BusyWait
                                    Thread.sleep(1000);
                                    System.exit(0);
                                }
                                Set<Integer> outOfOrderIdsNow = outOfOrderIds;
                                outOfOrderIds = new TreeSet<Integer>();
                                for (Integer idOutOfOrder : outOfOrderIdsNow) {
                                    if (idOutOfOrder == (lastId + 1)) {
                                        lastId = idOutOfOrder;
                                    } else {
                                        outOfOrderIds.add(idOutOfOrder);
                                    }
                                }
                            }
                        } else {
//                            log(lastId + " lastId but id= " + id);
                            outOfOrderIds.add(id);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    void log(Object obj) {
        logger.log(Level.INFO, "LRT-" + obj);
    }

    void addNode() {
        starts++;
        int entryCount = random(10000);
        int threadCount = random(10, 50);
        TheNode node = new TheNode(nodeIdGen++, entryCount, threadCount);
        nodes.add(node);
        node.start();
        log("Started " + node);
    }

    void restartNode() {
        restarts++;
        log("Restarting...");
        removeNode();
        try {
            Thread.sleep(random(10) * 1000);
        } catch (InterruptedException e) {
        }
        addNode();
    }

    void removeNode() {
        stops++;
        TheNode node = nodes.remove(random(nodes.size()));
        node.stop();
        log("Stopped " + node);
    }

    int random(int length) {
        return ((int) (Math.random() * 10000000) % length);
    }

    int random(int from, int to) {
        double diff = (to - from);
        return (int) (diff * Math.random() + from);
    }

    class TheNode {
        final int entryCount;
        final int threadCount;
        final int nodeId;
        final long createTime;
        final ExecutorService es;
        final ExecutorService esStats;
        final HazelcastInstance hazelcast;
        volatile boolean running = true;

        TheNode(int nodeId, int entryCount, int threadCount) {
            this.entryCount = entryCount;
            this.threadCount = threadCount;
            this.nodeId = nodeId;
            es = Executors.newFixedThreadPool(threadCount);
            hazelcast = Hazelcast.newHazelcastInstance(new Config());
            esStats = Executors.newSingleThreadExecutor();
            createTime = System.currentTimeMillis();
        }

        public void stop() {
            try {
                running = false;
                es.shutdown();
                es.awaitTermination(10, TimeUnit.SECONDS);
                esStats.shutdown();
                hazelcast.shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        public void start() {
            final Stats stats = new Stats();
            for (int i = 0; i < threadCount; i++) {
                es.execute(new Runnable() {
                    public void run() {
                        IQueue<Integer> q = hazelcast.getQueue("default");
                        Map<Integer, Integer> map1 = hazelcast.getMap("default");
                        Map<Integer, Integer> map2 = hazelcast.getMap("default");
                        Map<Integer, Integer> map3 = hazelcast.getMap("default");
                        Map<Integer, Integer> map4 = hazelcast.getMap("default");
                        while (running) {
                            try {
                                Transaction txn = hazelcast.getTransaction();
                                txn.begin();
                                try {
                                    int key = random(1000);
                                    while (key < 5) {
                                        key = random(1000);
                                    }
                                    Integer id = q.take();
                                    Integer id1 = map1.put(1, id);
                                    Integer id2 = map2.put(2, id);
                                    Integer id3 = map3.put(3, id);
                                    Integer id4 = map4.put(key, id);
                                    //noinspection BusyWait
                                    Thread.sleep(random.nextInt(5));
                                    txn.commit();
                                    processedIds.put(id);
                                } catch (Throwable e) {
                                    e.printStackTrace();
                                    txn.rollback();
                                }
                            } catch (Throwable ignored) {
                            }
                        }
                    }
                });
            }
            esStats.execute(new Runnable() {
                public void run() {
                    while (running) {
                        try {
                            //noinspection BusyWait
                            Thread.sleep(STATS_SECONDS * 1000);
                            int clusterSize = hazelcast.getCluster().getMembers().size();
                            Stats currentStats = stats.getAndReset();
                            log("Cluster size: " + clusterSize + ", Operations per Second: "
                                    + (currentStats.total() / STATS_SECONDS));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        @Override
        public String toString() {
            return "TheNode [" + nodeId +
                    "] entryCount=" + entryCount +
                    ", threadCount=" + threadCount +
                    ", liveSeconds=" + ((System.currentTimeMillis() - createTime) / 1000) +
                    ", running=" + running +
                    '}';
        }
    }

    class Stats {
        public AtomicLong mapPuts = new AtomicLong();
        public AtomicLong mapTakes = new AtomicLong();

        public Stats getAndReset() {
            long mapPutsNow = mapPuts.getAndSet(0);
            long mapRemovesNow = mapTakes.getAndSet(0);
            Stats newOne = new Stats();
            newOne.mapPuts.set(mapPutsNow);
            newOne.mapTakes.set(mapRemovesNow);
            return newOne;
        }

        public long total() {
            return mapPuts.get() + mapTakes.get();
        }

        public String toString() {
            return "total= " + total() + ", puts:" + mapPuts.get()
                    + ", remove:" + mapTakes.get();
        }
    }
}