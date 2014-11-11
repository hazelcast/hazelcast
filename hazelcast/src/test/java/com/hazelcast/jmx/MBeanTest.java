package com.hazelcast.jmx;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * This is the test that just tests if the mbeans get created. Tests in this class only exist for types that don't have
 * a specific test in this package. See LockMBeanTest for a specific test.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MBeanTest extends HazelcastTestSupport {
    private JmxTestDataHolder holder;

    @Before
    public void setUp() throws Exception {
        holder = new JmxTestDataHolder(createHazelcastInstanceFactory(1));
    }

    @Test
    public void testAtomicLong() throws Exception {
        IAtomicLong atomicLong = holder.getHz().getAtomicLong("atomiclong");
        atomicLong.incrementAndGet();

        holder.assertMBeanExistEventually("IAtomicLong", atomicLong.getName());
    }

    @Test
    public void testAtomicReference() throws Exception {
        IAtomicReference<String> atomicReference = holder.getHz().getAtomicReference("atomicreference");
        atomicReference.set(null);

        holder.assertMBeanExistEventually("IAtomicReference", atomicReference.getName());
    }

    @Test
    public void testSemaphore() throws Exception {
        ISemaphore semaphore = holder.getHz().getSemaphore("semaphore");
        semaphore.availablePermits();

        holder.assertMBeanExistEventually("ISemaphore", semaphore.getName());
    }

    @Test
    public void testCountDownLatch() throws Exception {
        ICountDownLatch countDownLatch = holder.getHz().getCountDownLatch("semaphore");
        countDownLatch.getCount();

        holder.assertMBeanExistEventually("ICountDownLatch", countDownLatch.getName());
    }

    @Test
    public void testMap() throws Exception {
        IMap map = holder.getHz().getMap("map");
        map.size();

        holder.assertMBeanExistEventually("IMap", map.getName());
    }

    @Test
    public void testMultiMap() throws Exception {
        MultiMap map = holder.getHz().getMultiMap("multimap");
        map.size();

        holder.assertMBeanExistEventually("MultiMap", map.getName());
    }

    @Test
    public void testTopic() throws Exception {
        ITopic<String> topic = holder.getHz().getTopic("topic");
        topic.publish("foo");

        holder.assertMBeanExistEventually("ITopic", topic.getName());
    }

    @Test
    public void testList() throws Exception {
        IList list = holder.getHz().getList("list");
        list.size();

        holder.assertMBeanExistEventually("IList", list.getName());
    }

    @Test
    public void testSet() throws Exception {
        ISet set = holder.getHz().getSet("set");
        set.size();

        holder.assertMBeanExistEventually("ISet", set.getName());
    }

    @Test
    public void testQueue() throws Exception {
        IQueue queue = holder.getHz().getQueue("queue");
        queue.size();

        holder.assertMBeanExistEventually("IQueue", queue.getName());
    }

    @Test
    public void testExecutor() throws Exception {
        IExecutorService executor = holder.getHz().getExecutorService("executor");
        executor.submit(new DummyRunnable()).get();

        holder.assertMBeanExistEventually("IExecutorService", executor.getName());
    }

    private static class DummyRunnable implements Runnable, Serializable {
        @Override
        public void run() {

        }
    }

}
