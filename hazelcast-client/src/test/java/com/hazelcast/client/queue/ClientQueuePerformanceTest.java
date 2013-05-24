package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @ali 5/21/13
 */
public class ClientQueuePerformanceTest {

    static final AtomicLong totalOffer = new AtomicLong();
    static final AtomicLong totalPoll = new AtomicLong();
    static final AtomicLong totalPeek = new AtomicLong();

    static final int THREAD_COUNT = 40;
    static final byte[] VALUE = new byte[1000];

    static HazelcastInstance server;
    static HazelcastInstance client;
    static IQueue q;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        server = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient(null);
        q = client.getQueue("test");
        test1();
    }

    public static void test1() throws Exception {
        final Random rnd = new Random(System.currentTimeMillis());
        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread() {
                public void run() {
                    while (true) {
                        int random = rnd.nextInt(100);
                        if (random > 54) {
                            q.poll();
                            totalPoll.incrementAndGet();
                        } else if (random > 4) {
                            q.offer(VALUE);
                            totalOffer.incrementAndGet();
                        } else {
                            q.peek();
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
                        int size = q.size();
                        if (size > 50000) {
                            System.err.println("cleaning a little");
                            for (int i = 0; i < 20000; i++) {
                                q.poll();
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

        while (true){
            long sleepTime = 10;
            Thread.sleep(sleepTime*1000);
            long totalOfferVal = totalOffer.getAndSet(0);
            long totalPollVal = totalPoll.getAndSet(0);
            long totalPeekVal = totalPeek.getAndSet(0);


            System.err.println("_______________________________________________________________________________________");
            System.err.println(" offer: " + totalOfferVal + ",\t poll: " + totalPollVal + ",\t peek: " + totalPeekVal);
            System.err.println(" size: " + q.size() + " \t speed: " + ((totalOfferVal+totalPollVal+totalPeekVal)/sleepTime));
            System.err.println("---------------------------------------------------------------------------------------");
            System.err.println("");
        }
    }

}
