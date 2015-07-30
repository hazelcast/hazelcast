package com.hazelcast.util;


import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class MPSCQueueTest extends HazelcastTestSupport {

//    @Test
//    public void test() throws InterruptedException {
//        FastQueue queue = new FastQueue(Thread.currentThread());
//        queue.offer("1");
//        queue.push("2");
//
//        Object[] drain = queue.takeAll();
//        List items = drainToList(drain);
//        assertEquals(asList("1", "2"), items);
//    }
//
//    public List drainToList(Object[] drain) {
//        Assert.assertNotNull(drain);
//
//        List list = new LinkedList();
//        for (int k = 0; k < drain.length; k++) {
//            Object item = drain[k];
//            if (item == null) {
//                break;
//            }
//
//            list.add(item);
//            drain[k] = null;
//        }
//        return list;
//    }
//
//
//    @Test
//    public void takeWhenEmpty() throws Exception {
//        TakeAllThread thread = new TakeAllThread();
//        FastQueue queue = thread.queue;
//        thread.start();
//
//        Thread.sleep(2000);
//
//        queue.push("1");
//
//        thread.join();
//
//        List items = drainToList(thread.result);
//        assertEquals(asList("1"), items);
//    }

    @Test
    public void producerConsumerTest() throws Exception {
        ConsumerThread consumerThread = new ConsumerThread();
        MPSCQueue queue = consumerThread.queue;
        int itemCount = 2000;
        ProducerThread producerThread = new ProducerThread(queue, itemCount);

        producerThread.start();
        consumerThread.start();

        producerThread.assertSucceedsEventually();
        consumerThread.assertSucceedsEventually();
        assertEquals(itemCount, consumerThread.itemCount);
    }

    class ProducerThread extends TestThread {
        private final MPSCQueue queue;
        private final int itemCount;

        public ProducerThread(MPSCQueue queue, int itemCount) {
            this.queue = queue;
            this.itemCount = itemCount;
        }

        public void doRun() {
            Random random = new Random();
            for (int k = 0; k < itemCount; k++) {
                queue.offer(k);
                //System.out.println("Produced: " + k);
                sleepMillis(random.nextInt(10));

                if (k % 100 == 0) {
                    System.out.println("Producer at " + k);
                }

            }
            queue.offer("exit");
        }
    }

    class ConsumerThread extends TestThread {
        private final MPSCQueue queue;
        private int itemCount;

        ConsumerThread() {
            queue = new MPSCQueue(this);
        }

        public void doRun() throws Exception {
            Random random = new Random();
            Integer expected = 0;
            for (; ; ) {
                Object item = queue.take();
                if (itemCount % 100 == 0) {
                    System.out.println("Consumer at " + itemCount);
                }

                //System.out.println("Consumed: " + item);

                if ("exit".equals(item)) {
                    break;
                }

                if (expected.equals(item)) {
                    expected++;
                } else {
                    System.out.println("Found a ordering problem,found: " + expected + " actual:" + item);
                    break;
                }
                itemCount++;

                sleepMillis(random.nextInt(10));
            }
        }
    }
}