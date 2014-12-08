package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICondition;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;


@RunWith(HazelcastParallelClassRunner.class)
@Ignore // https://github.com/hazelcast/hazelcast/issues/2272
public class ProducerConsumerConditionStressTest extends HazelcastTestSupport {

    private static volatile Object object;

    public static final int ITERATIONS = 1000000;
    public static final int PRODUCER_COUNT = 2;
    public static final int CONSUMER_COUNT = 2;
    public static final int INSTANCE_COUNT = 1;

    @Test
    public void test() {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(INSTANCE_COUNT).newInstances();
        HazelcastInstance hz = instances[0];
        //Hazelcast.newHazelcastInstance();
        ILock lock = hz.getLock(randomString());
        ICondition condition = lock.newCondition(randomString());

        ConsumerThread[] consumers = new ConsumerThread[CONSUMER_COUNT];
        for (int k = 0; k < consumers.length; k++) {
            ConsumerThread thread = new ConsumerThread(1, lock, condition);
            thread.start();
            consumers[k] = thread;
        }

        ProducerThread[] producers = new ProducerThread[PRODUCER_COUNT];
        for (int k = 0; k < producers.length; k++) {
            ProducerThread thread = new ProducerThread(1, lock, condition);
            thread.start();
            producers[k] = thread;
        }

        assertJoinable(600, producers);
        assertJoinable(600, consumers);

        for (TestThread consumer : consumers) {
            assertNull(consumer.throwable);
        }
        for (TestThread producer : producers) {
            assertNull(producer.throwable);
        }
    }

    abstract class TestThread extends Thread {
        private volatile Throwable throwable;

        TestThread(String name) {
            super(name);
        }

        public void run() {
            try {
                for (int k = 0; k < ITERATIONS; k++) {
                    runSingleIteration();

                    if (k % 100 == 0) {
                        System.out.println(getName() + " is at: " + k);
                    }
                }
            } catch (Throwable e) {
                this.throwable = e;
                e.printStackTrace();
            }
        }

        abstract void runSingleIteration() throws InterruptedException;
    }

    class ProducerThread extends TestThread {
        private final ILock lock;
        private final ICondition condition;

        ProducerThread(int id, ILock lock, ICondition condition) {
            super("ProducerThread-" + id);
            this.lock = lock;
            this.condition = condition;
        }


        void runSingleIteration() throws InterruptedException {
            lock.lock();

            try {
                while (object != null) {
                    condition.await();
                }
                object = "";
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    class ConsumerThread extends TestThread {
        private final ILock lock;
        private final ICondition condition;

        ConsumerThread(int id, ILock lock, ICondition condition) {
            super("ConsumerThread-" + id);
            this.lock = lock;
            this.condition = condition;
        }

        void runSingleIteration() throws InterruptedException {
            lock.lock();

            try {
                while (object == null) {
                    condition.await();
                }
                object = null;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
