package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RingbufferAddAllReadManyStressTest extends HazelcastTestSupport {

    public static final int MAX_BATCH = 100;
    private final AtomicBoolean stop = new AtomicBoolean();
    private Ringbuffer<Long> ringbuffer;

    @Test
    public void whenNoTTL() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(20 * 1000 * 1000)
                .setTimeToLiveSeconds(0);
        test(ringbufferConfig);
    }

    @Test
    public void whenTTLEnabled() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(200 * 1000)
                .setTimeToLiveSeconds(2);
        test(ringbufferConfig);
    }

    @Test
    public void whenLongTTLAndSmallBuffer() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(1000)
                .setTimeToLiveSeconds(30);
        test(ringbufferConfig);
    }

    @Test
    public void whenShortTTLAndBigBuffer() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(20 * 1000 * 1000)
                .setTimeToLiveSeconds(2);
        test(ringbufferConfig);
    }

    public void test(RingbufferConfig ringbufferConfig) throws Exception {
        Config config = new Config();
        config.addRingBufferConfig(ringbufferConfig);
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances(config);

        ringbuffer = instances[0].getRingbuffer(ringbufferConfig.getName());
        ConsumeThread consumer1 = new ConsumeThread(1);
        consumer1.start();

        ConsumeThread consumer2 = new ConsumeThread(2);
        consumer2.start();

        sleepSeconds(2);

        ProduceThread producer = new ProduceThread();
        producer.start();

        sleepAndStop(stop, 5 * 60);
        System.out.println("Waiting fo completion");

        producer.assertSucceedsEventually();
        consumer1.assertSucceedsEventually();
        consumer2.assertSucceedsEventually();

        System.out.println("producer.produced:" + producer.produced);

        assertEquals(producer.produced, consumer1.seq);
        assertEquals(producer.produced, consumer2.seq);
    }

    class ProduceThread extends TestThread {
        private volatile long produced;

        public ProduceThread() {
            super("ProduceThread");
        }

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() throws Throwable {
            Random random = new Random();

            while (!stop.get()) {
                LinkedList<Long> items = makeBatch(random);
                addAll(items);
            }

            ringbuffer.add(Long.MIN_VALUE);
        }

        private LinkedList<Long> makeBatch(Random random) {
            int count = max(1, random.nextInt(MAX_BATCH));
            LinkedList<Long> items = new LinkedList<Long>();
            for (int k = 0; k < count; k++) {
                items.add(produced);
                produced++;

                if (produced % 100000 == 0) {
                    System.out.println(getName() + " at " + produced);
                }
            }
            return items;
        }

        private void addAll(LinkedList<Long> items) throws InterruptedException, java.util.concurrent.ExecutionException {
            long sleepMs = 100;
            for (; ; ) {
                long result = ringbuffer.addAllAsync(items, FAIL).get();
                if (result != -1) {
                    break;
                }
                System.out.println("Backoff");
                TimeUnit.MILLISECONDS.sleep(sleepMs);
                sleepMs = sleepMs * 2;
                if (sleepMs > 1000) {
                    sleepMs = 1000;
                }
            }
        }
    }

    class ConsumeThread extends TestThread {
        volatile long seq;

        public ConsumeThread(int id) {
            super("ConsumeThread-" + id);
        }

        @Override
        public void onError(Throwable t) {
            stop.set(true);
        }

        @Override
        public void doRun() throws Throwable {
            seq = ringbuffer.headSequence();

            Random random = new Random();

            for (; ; ) {
                int max = max(1, random.nextInt(MAX_BATCH));
                ReadResultSet<Long> result = ringbuffer.readManyAsync(seq, 1, max, null).get();
                for (Long item : result) {
                    if (item.equals(Long.MIN_VALUE)) {
                        return;
                    }

                    assertEquals(new Long(seq), item);
                    seq++;

                    if (seq % 100000 == 0) {
                        System.out.println(getName() + " at " + seq);
                    }
                }
            }
        }
    }
}
