package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MBeanTest extends HazelcastTestSupport {

    private static HazelcastInstance hz;
    private static MBeanServer mbs;

    @BeforeClass
    public static void setUp() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_ENABLE_JMX, "true");
        hz = new TestHazelcastInstanceFactory(1).newHazelcastInstance(config);
        mbs = ManagementFactory.getPlatformMBeanServer();
    }

    public void assertMBeanExistEventually(final String type, final String name) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Hashtable table = new Hashtable();
                table.put("type", type);
                table.put("name", name);
                table.put("instance", hz.getName());
                ObjectName name = new ObjectName("com.hazelcast", table);
                try {
                    ObjectInstance mbean = mbs.getObjectInstance(name);
                } catch (InstanceNotFoundException e) {
                    fail(e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAtomicLong() throws Exception {
        IAtomicLong atomicLong = hz.getAtomicLong("atomiclong");
        atomicLong.incrementAndGet();

        assertMBeanExistEventually("IAtomicLong", atomicLong.getName());
    }

    @Test
    public void testAtomicReference() throws Exception {
        IAtomicReference atomicReference = hz.getAtomicReference("atomicreference");
        atomicReference.set(null);

        assertMBeanExistEventually("IAtomicReference", atomicReference.getName());
    }

    @Test
    public void testLock() throws Exception {
        ILock lock = hz.getLock("lock");
        lock.tryLock();

        assertMBeanExistEventually("ILock", lock.getName());
    }

    @Test
    public void testSemaphore() throws Exception {
        ISemaphore semaphore = hz.getSemaphore("semaphore");
        semaphore.availablePermits();

        assertMBeanExistEventually("ISemaphore", semaphore.getName());
    }

    @Test
    public void testCountDownLatch() throws Exception {
        ICountDownLatch countDownLatch = hz.getCountDownLatch("semaphore");
        countDownLatch.getCount();

        assertMBeanExistEventually("ICountDownLatch", countDownLatch.getName());
    }

    @Test
    public void testMap() throws Exception {
        IMap map = hz.getMap("map");
        map.size();

        assertMBeanExistEventually("IMap", map.getName());
    }

    @Test
    public void testMultiMap() throws Exception {
        MultiMap map = hz.getMultiMap("multimap");
        map.size();

        assertMBeanExistEventually("MultiMap", map.getName());
    }

    @Test
    public void testTopic() throws Exception {
        ITopic topic = hz.getTopic("topic");
        topic.publish("foo");

        assertMBeanExistEventually("ITopic", topic.getName());
    }

    @Test
    public void testList() throws Exception {
        IList list = hz.getList("list");
        list.size();

        assertMBeanExistEventually("IList", list.getName());
    }

    @Test
    public void testSet() throws Exception {
        ISet set = hz.getSet("set");
        set.size();

        assertMBeanExistEventually("ISet", set.getName());
    }

    @Test
    public void testQueue() throws Exception {
        IQueue queue = hz.getQueue("queue");
        queue.size();

        assertMBeanExistEventually("IQueue", queue.getName());
    }

    @Test
    public void testExecutor() throws Exception {
        IExecutorService executor = hz.getExecutorService("executor");
        executor.submit(new DummyRunnable()).get();

        assertMBeanExistEventually("IExecutorService", executor.getName());
    }

    private static class DummyRunnable implements Runnable, Serializable {
        @Override
        public void run() {

        }
    }

}
