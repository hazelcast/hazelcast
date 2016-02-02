package com.hazelcast.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestThread;
import org.junit.Test;

import static org.junit.Assert.fail;

public class LockInterruptTest extends HazelcastTestSupport {

    @Test
    public void test() {
        HazelcastInstance hz = createHazelcastInstance();
        final ILock lock = hz.getLock("foo");
        lock.lock();

        TestThread thread = new TestThread() {
            public void doRun() throws Exception {
                try {
                    lock.lockInterruptibly();
                    fail();
                } catch (InterruptedException t) {
                    t.printStackTrace();
                }
            }
        };
        thread.start();
        sleepSeconds(1);
        thread.interrupt();
        thread.assertSucceedsEventually();
    }
}
