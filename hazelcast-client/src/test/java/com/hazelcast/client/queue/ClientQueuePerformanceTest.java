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

package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.junit.Ignore;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ali 5/21/13
 */
@Ignore("not a JUnit test")
public class ClientQueuePerformanceTest {

    private static final int THREAD_COUNT = 40;
    private static final byte[] VALUE = new byte[1000];

    private static final AtomicLong totalOffer = new AtomicLong();
    private static final AtomicLong totalPoll = new AtomicLong();
    private static final AtomicLong totalPeek = new AtomicLong();

    private static HazelcastInstance server;

    private static IQueue<Object> queue;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient(null);
        queue = client.getQueue("test");
        //test1();
        test2();
    }

    private static void test1() throws Exception {
        final Random rnd = new Random();
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread() {
                public void run() {
                    while (true) {
                        int random = rnd.nextInt(100);
                        if (random > 54) {
                            queue.poll();
                            totalPoll.incrementAndGet();
                        } else if (random > 4) {
                            queue.offer(VALUE);
                            totalOffer.incrementAndGet();
                        } else {
                            queue.peek();
                            totalPeek.incrementAndGet();
                        }
                    }
                }
            }.start();
        }

        new Thread() {
            public void run() {
                while (true) {
                    try {
                        int size = queue.size();
                        if (size > 50000) {
                            System.err.println("cleaning a little");
                            for (int i = 0; i < 20000; i++) {
                                queue.poll();
                                totalPoll.incrementAndGet();
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
            long sleepTime = 10;
            Thread.sleep(sleepTime * 1000);
            long totalOfferVal = totalOffer.getAndSet(0);
            long totalPollVal = totalPoll.getAndSet(0);
            long totalPeekVal = totalPeek.getAndSet(0);

            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal + ",\t peek: " + totalPeekVal);
            System.err.println(" size: " + queue.size() + " \t speed: " + ((totalOfferVal + totalPollVal + totalPeekVal) / sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println();
        }
    }

    private static void test2() throws Exception {
        final CountDownLatch latch = new CountDownLatch(100);
        final CountDownLatch latch1 = new CountDownLatch(1000);
        new Thread() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    queue.offer("item" + i);
                    latch.countDown();
                    latch1.countDown();
                }
            }
        }.start();

        latch.await();

        server.shutdown();

        latch1.await();

        System.err.println("size: " + queue.size());
    }
}
