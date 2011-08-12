/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.examples;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.partition.Partition;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

public class SimpleMapTest {

    public static final int STATS_SECONDS = 10;
    public static int THREAD_COUNT = 40;
    public static int ENTRY_COUNT = 10 * 1000;
    public static int VALUE_SIZE = 1000;
    public static int GET_PERCENTAGE = 40;
    public static int PUT_PERCENTAGE = 40;

    public static void main(String[] args) {
        final ILogger logger = Hazelcast.getLoggingService().getLogger("SimpleMapTest");
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
            logger.log(Level.INFO, "Help: sh test.sh t200 v130 p10 g85 ");
            logger.log(Level.INFO, "    // means 200 threads, value-size 130 bytes, 10% put, 85% get");
            logger.log(Level.INFO, "");
        }
        logger.log(Level.INFO, "Starting Test with ");
        logger.log(Level.INFO, "      Thread Count: " + THREAD_COUNT);
        logger.log(Level.INFO, "       Entry Count: " + ENTRY_COUNT);
        logger.log(Level.INFO, "        Value Size: " + VALUE_SIZE);
        logger.log(Level.INFO, "    Get Percentage: " + GET_PERCENTAGE);
        logger.log(Level.INFO, "    Put Percentage: " + PUT_PERCENTAGE);
        logger.log(Level.INFO, " Remove Percentage: " + (100 - (PUT_PERCENTAGE + GET_PERCENTAGE)));
        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        final IMap<String, byte[]> map = Hazelcast.getMap("default");
        if (load) {
            final Member thisMember = Hazelcast.getCluster().getLocalMember();
            for (int i = 0; i < ENTRY_COUNT; i++) {
                final String key = String.valueOf(i);
                Partition partition = Hazelcast.getPartitionService().getPartition(key);
                if (thisMember.equals(partition.getOwner())) {
                    es.execute(new Runnable() {
                        public void run() {
                            map.put(key, new byte[VALUE_SIZE]);
                        }
                    });
                }
            }
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            es.execute(new Runnable() {
                public void run() {
                    while (true) {
                        int key = (int) (Math.random() * ENTRY_COUNT);
                        int operation = ((int) (Math.random() * 100));
                        if (operation < GET_PERCENTAGE) {
                            map.get(String.valueOf(key));
                        } else if (operation < GET_PERCENTAGE + PUT_PERCENTAGE) {
                            map.put(String.valueOf(key), new byte[VALUE_SIZE]);
                        } else {
                            map.remove(String.valueOf(key));
                        }
                    }
                }
            });
        }
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        logger.log(Level.INFO, "cluster size:" + Hazelcast.getCluster().getMembers().size());
                        LocalMapOperationStats mapOpStats = map.getLocalMapStats().getOperationStats();
                        long period = ((mapOpStats.getPeriodEnd() - mapOpStats.getPeriodStart()) / 1000);
                        if (period == 0) {
                            continue;
                        }
                        logger.log(Level.INFO, mapOpStats.toString());
                        logger.log(Level.INFO, "Operations per Second : " + mapOpStats.total() / period);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        });
    }
}
