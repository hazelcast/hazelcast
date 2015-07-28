package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import com.hazelcast.test.annotation.NightlyTest;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class RingbufferAddAllReadManyStressTest extends HazelcastTestSupport {

    public static final int MAX_BATCH = 100;
    private final AtomicBoolean stop = new AtomicBoolean();
    private Ringbuffer<Long> ringbuffer;

    @After
    public void tearDown() {
        if (ringbuffer != null) {
            ringbuffer.destroy();
        }
    }

    @Test
    public void whenNoTTL() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("rb")
                .setCapacity(20 * 1000 * 1000)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0);
        test(ringbufferConfig);
    }

    @Test
    public void whenTTLEnabled() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("rb")
                .setCapacity(200 * 1000)
                .setTimeToLiveSeconds(2);
        test(ringbufferConfig);
    }

    @Test
    public void whenLongTTLAndSmallBuffer() throws Exception {
        RingbufferConfig ringbufferConfig = new RingbufferConfig("rb")
                .setCapacity(1000)
                .setTimeToLiveSeconds(30);
        test(ringbufferConfig);
    }

    @Test
    public void whenShortTTLAndBigBuffer() throws Exception {

        RingbufferConfig ringbufferConfig = new RingbufferConfig("rb")
                .setInMemoryFormat(InMemoryFormat.OBJECT)
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

        sleepAndStop(stop, 60);
        System.out.println("Waiting fo completion");

        producer.assertSucceedsEventually();
        consumer1.assertSucceedsEventually();
        consumer2.assertSucceedsEventually();

        System.out.println("producer.produced:" + producer.produced);

        assertEquals(producer.produced, consumer1.seq);
        assertEquals(producer.produced, consumer2.seq);
    }

    class ProduceThread extends TestThread {
        private final ILogger logger = Logger.getLogger(ProduceThread.class);
        private volatile long produced;
        long lastLogMs = 0;


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

                long currentTimeMs = currentTimeMillis();
                if (lastLogMs + SECONDS.toMillis(5) < currentTimeMs) {
                    lastLogMs = currentTimeMs;
                    logger.info(getName() + " at " + produced);
                }
            }
            return items;
        }

        private void addAll(LinkedList<Long> items) throws InterruptedException, ExecutionException {
            long sleepMs = 100;
            for (; ; ) {
                long result = ringbuffer.addAllAsync(items, FAIL).get();
                if (result != -1) {
                    break;
                }
                logger.info("Backoff");
                MILLISECONDS.sleep(sleepMs);
                sleepMs = sleepMs * 2;
                if (sleepMs > 1000) {
                    sleepMs = 1000;
                }
            }
        }
    }

    class ConsumeThread extends TestThread {
        private final ILogger logger = Logger.getLogger(ConsumeThread.class);
        volatile long seq;
        long lastLogMs = 0;

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

                    long currentTimeMs = currentTimeMillis();
                    if (lastLogMs + SECONDS.toMillis(5) < currentTimeMs) {
                        lastLogMs = currentTimeMs;
                        logger.info(getName() + " at " + seq);
                    }
                    seq++;
                }
            }
        }
    }
}
