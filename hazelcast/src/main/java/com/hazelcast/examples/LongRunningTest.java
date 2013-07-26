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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.logging.ILogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LongRunningTest {

    private static final int STATS_SECONDS = 10;
    private static final Logger logger = Logger.getLogger(LongRunningTest.class.getName());

    private final List<TheNode> nodes = new CopyOnWriteArrayList<TheNode>();
    private int nodeIdGen = 0;
    private int starts, stops, restarts = 0;
    private int maxNodeSize = 4;
    private int minNodeSize = 2;
    private int nextActionMin = 90;
    private int nextActionMax = 180;

    public LongRunningTest(String[] input) {
        if (input != null && input.length > 0) {
            for (String arg : input) {
                arg = arg.trim();
                if (arg.startsWith("-n1=")) {
                    minNodeSize = Integer.parseInt(arg.substring(4));
                } else if (arg.startsWith("-n2=")) {
                    maxNodeSize = Integer.parseInt(arg.substring(4));
                } else if (arg.startsWith("-i1=")) {
                    nextActionMin = Integer.parseInt(arg.substring(4));
                } else if (arg.startsWith("-i2=")) {
                    nextActionMax = Integer.parseInt(arg.substring(4));
                }
            }
        }
    }

    public static void main(String[] args) {
        LongRunningTest t = new LongRunningTest(args);
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
        log("Min node size: " + minNodeSize);
        log("Max node size: " + maxNodeSize);
        for (int i = 0; i < minNodeSize; i++) {
            addNode();
        }

        while (!Thread.currentThread().isInterrupted()) {
            if (nodes.size() >= maxNodeSize) {
                removeNode();
            } else if (nodes.size() < minNodeSize) {
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
                int nextSeconds = random(nextActionMin, nextActionMax);
                log("Next Action after " + nextSeconds + " seconds.");
                log("members:" + nodes.size() + ", starts: " + starts + ", stops:" + stops + ", restart:" + restarts);
                Thread.sleep(nextSeconds * 1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    void log(Object obj) {
        logger.info(obj.toString());
    }

    void addNode() {
        starts++;
        int entryCount = random(10000);
        int threadCount = random(10, 50);
        int valueSizeMax = (entryCount < 1000) ? 50000 : 1000;
        int valueSize = random(10, valueSizeMax);
        TheNode node = new TheNode(nodeIdGen++, entryCount, threadCount, valueSize);
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
        final int valueSize;
        final int nodeId;
        final long createTime;
        final ExecutorService es;
        final ExecutorService esStats;
        final HazelcastInstance hazelcast;
        volatile boolean running = true;

        TheNode(int nodeId, int entryCount, int threadCount, int valueSize) {
            this.entryCount = entryCount;
            this.threadCount = threadCount;
            this.valueSize = valueSize;
            this.nodeId = nodeId;
            es = Executors.newFixedThreadPool(threadCount);
            Config cfg = new XmlConfigBuilder().build();
            hazelcast = Hazelcast.newHazelcastInstance(cfg);
            esStats = Executors.newSingleThreadExecutor();
            createTime = System.currentTimeMillis();
        }

        public void stop() {
            try {
                running = false;
                es.shutdown();
                es.awaitTermination(10, TimeUnit.SECONDS);
                esStats.shutdown();
                hazelcast.getLifecycleService().shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        public void start() {
            final Stats stats = new Stats();
            for (int i = 0; i < threadCount; i++) {
                es.submit(new Runnable() {
                    public void run() {
                        Map<String, byte[]> map = hazelcast.getMap("default");
                        while (running) {
                            try {
                                int key = (int) (Math.random() * entryCount);
                                int operation = ((int) (Math.random() * 100)) % 10;
                                if (operation < 4) {
                                    map.put(String.valueOf(key), new byte[valueSize]);
                                    stats.mapPuts.incrementAndGet();
                                } else if (operation < 8) {
                                    map.get(String.valueOf(key));
                                    stats.mapGets.incrementAndGet();
                                } else {
                                    map.remove(String.valueOf(key));
                                    stats.mapRemoves.incrementAndGet();
                                }
                            } catch (HazelcastInstanceNotActiveException ignored) {
                            } catch (Throwable e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
            esStats.submit(new Runnable() {
                public void run() {
                    final ILogger logger = hazelcast.getLoggingService().getLogger(hazelcast.getName());
                    while (running) {
                        try {
                            Thread.sleep(STATS_SECONDS * 1000);
                            int clusterSize = hazelcast.getCluster().getMembers().size();
                            Stats currentStats = stats.getAndReset();
                            logger.info("Cluster size: " + clusterSize + ", Operations per Second: "
                                    + (currentStats.total() / STATS_SECONDS));
                        } catch (HazelcastInstanceNotActiveException ignored) {
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        @Override
        public String toString() {
            return "TheNode{" +
                    "nodeId=" + nodeId +
                    ", entryCount=" + entryCount +
                    ", threadCount=" + threadCount +
                    ", valueSize=" + valueSize +
                    ", liveSeconds=" + ((System.currentTimeMillis() - createTime) / 1000) +
                    ", running=" + running +
                    '}';
        }
    }

    class Stats {
        public AtomicLong mapPuts = new AtomicLong();
        public AtomicLong mapGets = new AtomicLong();
        public AtomicLong mapRemoves = new AtomicLong();

        public Stats getAndReset() {
            long mapPutsNow = mapPuts.getAndSet(0);
            long mapGetsNow = mapGets.getAndSet(0);
            long mapRemovesNow = mapRemoves.getAndSet(0);
            Stats newOne = new Stats();
            newOne.mapPuts.set(mapPutsNow);
            newOne.mapGets.set(mapGetsNow);
            newOne.mapRemoves.set(mapRemovesNow);
            return newOne;
        }

        public long total() {
            return mapPuts.get() + mapGets.get() + mapRemoves.get();
        }

        public String toString() {
            return "total= " + total() + ", puts:" + mapPuts.get() + ", gets:" + mapGets.get()
                    + ", remove:" + mapRemoves.get();
        }
    }

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.socket.bind.any", "false");
        System.setProperty("hazelcast.partition.migration.interval", "0");
    }
}
