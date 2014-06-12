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

package com.hazelcast.queue.standalone;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple queue test
 */
public final class SimpleQueueTest {

    private static final int VALUE_SIZE = 1000;
    private static final int STATS_SECONDS = 10;

    private SimpleQueueTest() {
    }

    /**
     * Creates a cluster and exercises a queue until stopped
     * @param args none
     */
    public static void main(String[] args) {
        int threadCount = 5;
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        final Stats stats = new Stats();
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            es.submit(new Runnable() {
                public void run() {
                    Random random = new Random();
                    while (true) {
                        int ran = random.nextInt(100);
                        Queue<byte[]> queue = hz1.getQueue("default" + ran);
                        for (int j = 0; j < 1000; j++) {
                            queue.offer(new byte[VALUE_SIZE]);
                            stats.offers.incrementAndGet();
                        }
                        for (int j = 0; j < 1000; j++) {
                            queue.poll();
                            stats.polls.incrementAndGet();
                        }
                    }
                }
            });
        }

        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @SuppressWarnings("BusyWait")
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("cluster size:"
                                + hz1.getCluster().getMembers().size());
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

    /**
     * A basic statistics class
     */
    public static class Stats {

        private AtomicLong offers = new AtomicLong();
        private AtomicLong polls = new AtomicLong();

        public Stats getAndReset() {
            long offersNow = offers.getAndSet(0);
            long pollsNow = polls.getAndSet(0);
            Stats newOne = new Stats();
            newOne.offers.set(offersNow);
            newOne.polls.set(pollsNow);
            return newOne;
        }

        public long total() {
            return offers.get() + polls.get();
        }

        public String toString() {
            return "total= " + total() + ", offers:" + offers.get() + ", polls:" + polls.get();
        }
    }
}
