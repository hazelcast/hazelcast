/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ClientQueuePerformanceBenchmark extends HazelcastTestSupport {

    private static final AtomicLong TOTAL_OFFER = new AtomicLong();
    private static final AtomicLong TOTAL_POLL = new AtomicLong();
    private static final AtomicLong TOTAL_PEEK = new AtomicLong();

    private static final int THREAD_COUNT = 40;
    private static final byte[] VALUE = new byte[1000];

    private static HazelcastInstance server;
    private static IQueue<Object> queue;

    public static void main(String[] args) {
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        queue = client.getQueue("test");
        //test1();
        test2();
    }

    private static void test1() {
        final Random rnd = new Random();
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread() {
                @Override
                public void run() {
                    while (true) {
                        int random = rnd.nextInt(100);
                        if (random > 54) {
                            queue.poll();
                            TOTAL_POLL.incrementAndGet();
                        } else if (random > 4) {
                            queue.offer(VALUE);
                            TOTAL_OFFER.incrementAndGet();
                        } else {
                            queue.peek();
                            TOTAL_PEEK.incrementAndGet();
                        }
                    }
                }
            }.start();
        }

        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        int size = queue.size();
                        if (size > 50000) {
                            System.err.println("cleaning a little");
                            for (int i = 0; i < 20000; i++) {
                                queue.poll();
                                TOTAL_POLL.incrementAndGet();
                            }
                            Thread.sleep(2 * 1000);
                        } else {
                            Thread.sleep(10 * 1000);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        while (true) {
            int sleepTime = 10;
            sleepSeconds(sleepTime);
            long totalOfferVal = TOTAL_OFFER.getAndSet(0);
            long totalPollVal = TOTAL_POLL.getAndSet(0);
            long totalPeekVal = TOTAL_PEEK.getAndSet(0);

            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal + ",\t peek: " + totalPeekVal);
            System.err.println(" size: " + queue.size()
                    + " \t speed: " + ((totalOfferVal + totalPollVal + totalPeekVal) / sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println("");
        }
    }

    private static void test2() {
        final CountDownLatch latch1 = new CountDownLatch(100);
        final CountDownLatch latch2 = new CountDownLatch(1000);
        new Thread() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    queue.offer("item" + i);
                    latch1.countDown();
                    latch2.countDown();
                }
            }
        }.start();

        assertOpenEventually(latch1);

        server.shutdown();

        assertOpenEventually(latch2);

        System.err.println("size: " + queue.size());
    }
}
