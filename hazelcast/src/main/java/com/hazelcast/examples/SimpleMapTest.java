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
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.partition.Partition;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class SimpleMapTest {

    private static final String NAMESPACE = "default";
    private static final long STATS_SECONDS = 10;

    private final HazelcastInstance instance;
    private final ILogger logger;
    private final Stats stats = new Stats();

    private final int threadCount;
    private final int entryCount;
    private final int valueSize;
    private final int getPercentage;
    private final int putPercentage;
    private final boolean load;

    static {
        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("java.net.preferIPv4Stack", "true");
    }

    public SimpleMapTest(final int threadCount, final int entryCount, final int valueSize,
                         final int getPercentage, final int putPercentage, final boolean load) {
        this.threadCount = threadCount;
        this.entryCount = entryCount;
        this.valueSize = valueSize;
        this.getPercentage = getPercentage;
        this.putPercentage = putPercentage;
        this.load = load;
        Config cfg = new XmlConfigBuilder().build();
        cfg.getMapConfig(NAMESPACE).setStatisticsEnabled(true);
        cfg.getSerializationConfig().setPortableFactory(new PortableFactory() {
            public Portable create(int classId) {
                return new PortableByteArray();
            }
        });
        instance = Hazelcast.newHazelcastInstance(cfg);
        logger = instance.getLoggingService().getLogger("SimpleMapTest");
    }

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

        SimpleMapTest test = new SimpleMapTest(threadCount, entryCount, valueSize, getPercentage, putPercentage, load);
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
        final IMap<String, Object> map = instance.getMap(NAMESPACE);
        for (int i = 0; i < threadCount; i++) {
            es.execute(new Runnable() {
                public void run() {
                    try {
                        while (true) {
                            int key = (int) (Math.random() * entryCount);
                            int operation = ((int) (Math.random() * 100));
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
                    } catch (HazelcastInstanceNotActiveException ignored) {
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    private Object createValue() {
        return new byte[valueSize];
//        return new DataSerializableByteArray(new byte[valueSize]);
//        return new PortableByteArray(new byte[valueSize]);
    }

    private void load(ExecutorService es) throws InterruptedException {
        if (!load) return;

        final IMap<String, Object> map = instance.getMap(NAMESPACE);
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
        new Thread() {
            {
                setDaemon(true);
                setName("PrintStats." + instance.getName());
            }

            public void run() {
                final IMap<String, Object> map = instance.getMap(NAMESPACE);
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        stats.printAndReset();
                        final LocalMapStats localMapStats = map.getLocalMapStats();
                        logger.log(Level.INFO, "Owned-Entries= " + localMapStats.getOwnedEntryCount()
                                + ", Backup-Entries:" + localMapStats.getBackupEntryCount() + '\n');
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        }.start();
    }

    private class Stats {
        public AtomicLong gets = new AtomicLong();
        public AtomicLong puts = new AtomicLong();
        public AtomicLong removes = new AtomicLong();

        public void printAndReset() {
            long getsNow = gets.getAndSet(0);
            long putsNow = puts.getAndSet(0);
            long removesNow = removes.getAndSet(0);
            long total = getsNow + putsNow + removesNow;

            logger.log(Level.INFO, "total= " + total + ", gets:" + getsNow
                    + ", puts:" + putsNow + ", removes:" + removesNow);
            logger.log(Level.INFO, "Operations per Second : " + total / STATS_SECONDS);
        }
    }

    private static class DataSerializableByteArray implements DataSerializable {

        byte[] data;

        public DataSerializableByteArray() {
        }

        private DataSerializableByteArray(byte[] data) {
            this.data = data;
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            int len = data != null ? data.length : 0;
            out.writeInt(len);
            if (len > 0) {
                for (int i = 0; i < len; i++) {
                    out.writeByte(data[i]);
                }
            }
        }

        public void readData(ObjectDataInput in) throws IOException {
            int len = in.readInt();
            if (len > 0) {
                data = new byte[len];
                for (int i = 0; i < len; i++) {
                    data[i] = in.readByte();
                }
            }
        }
    }

    private static class PortableByteArray implements Portable {

        byte[] data;

        public PortableByteArray() {
        }

        private PortableByteArray(byte[] data) {
            this.data = data;
        }

        public int getClassId() {
            return 1;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByteArray("d", data);
        }

        public void readPortable(PortableReader reader) throws IOException {
            data = reader.readByteArray("d");
        }
    }

    private void printVariables() {
        logger.log(Level.INFO, "Starting Test with ");
        logger.log(Level.INFO, "Thread Count: " + threadCount);
        logger.log(Level.INFO, "Entry Count: " + entryCount);
        logger.log(Level.INFO, "Value Size: " + valueSize);
        logger.log(Level.INFO, "Get Percentage: " + getPercentage);
        logger.log(Level.INFO, "Put Percentage: " + putPercentage);
        logger.log(Level.INFO, "Remove Percentage: " + (100 - (putPercentage + getPercentage)));
        logger.log(Level.INFO, "Load: " + load);
    }
}
