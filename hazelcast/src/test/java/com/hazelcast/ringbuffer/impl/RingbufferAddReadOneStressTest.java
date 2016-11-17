package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RingbufferAddReadOneStressTest extends HazelcastTestSupport {

    private final AtomicBoolean stop = new AtomicBoolean();
    private Ringbuffer<Long> ringbuffer;

    @Test
    public void whenNoTTL() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("foo")
                .setCapacity(200 * 1000)
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
                .setCapacity(3 * 1000)
                .setTimeToLiveSeconds(30);
        test(ringbufferConfig);
    }

    @Ignore //https://github.com/hazelcast/hazelcast/issues/6895
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

        System.out.println("Starting test");
        ConsumeThread consumer1 = new ConsumeThread(1);
        consumer1.start();

        ConsumeThread consumer2 = new ConsumeThread(2);
        consumer2.start();

        sleepSeconds(2);

        ProduceThread producer = new ProduceThread();
        producer.start();

        long startMs = System.currentTimeMillis();

        sleepAndStop(stop, 3 * 60);
        System.out.println("Waiting for completion");

        producer.assertSucceedsEventually();
        consumer1.assertSucceedsEventually();
        consumer2.assertSucceedsEventually();

        System.out.println("producer.produced:" + producer.produced);

        assertEquals(producer.produced, consumer1.seq);
        assertEquals(producer.produced, consumer2.seq);

        long durationMs = System.currentTimeMillis() - startMs;
        double performance = (1000d * producer.produced) / durationMs;
        System.out.println("Performance: " + performance + " messages per second");
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
            long prev = System.currentTimeMillis();
            while (!stop.get()) {
                ringbuffer.add(produced);

                produced++;

                long now = System.currentTimeMillis();
                if (now > prev + 2000) {
                    prev = now;
                    System.out.println(getName() + " at " + produced);
                }
            }

            ringbuffer.add(Long.MIN_VALUE);
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
            long prev = System.currentTimeMillis();

            for (; ; ) {
                Long item = ringbuffer.readOne(seq);
                if (item.equals(Long.MIN_VALUE)) {
                    break;
                }

                assertEquals(new Long(seq), item);

                seq++;

                long now = System.currentTimeMillis();
                if (now > prev + 2000) {
                    prev = now;
                    System.out.println(getName() + " at " + seq);
                }
            }
        }
    }
}
