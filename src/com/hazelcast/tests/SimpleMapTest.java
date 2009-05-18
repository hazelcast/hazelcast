/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.tests;

import com.hazelcast.core.Hazelcast;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleMapTest {

    public static final int ENTRY_COUNT = 10 * 1000;
    public static final int VALUE_SIZE = 1000;
    public static final int STATS_SECONDS = 10;

    public static void main(String[] args) {
        int threadCount = 40;
        final Stats stats = new Stats();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            es.submit(new Runnable() {
                public void run() {
                    Map<String, byte[]> map = Hazelcast.getMap("default");
                    while (true) {
                        int key = (int) (Math.random() * ENTRY_COUNT);
                        int operation = ((int) (Math.random() * 100)) % 10;
                        if (operation < 4) {
                            map.put(String.valueOf(key), new byte[VALUE_SIZE]);
                            stats.mapPuts.incrementAndGet();
                        }
                        else if (operation < 8) {
                            map.get(String.valueOf(key));
                            stats.mapGets.incrementAndGet();
                        }
                        else {
                            map.remove(String.valueOf(key));
                            stats.mapRemoves.incrementAndGet();
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
                                + Hazelcast.getCluster().getMembers().size());
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
}
