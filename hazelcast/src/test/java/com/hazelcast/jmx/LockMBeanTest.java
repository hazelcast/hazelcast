package com.hazelcast.jmx;

import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LockMBeanTest extends HazelcastTestSupport {
    private static JmxTestDataHolder holder;

    @BeforeClass
    public static void setUp() throws Exception {
        holder = new JmxTestDataHolder();
    }

    @Test
    public void testLock() throws Exception {
        ILock lock = holder.getHz().getLock("lock");
        // Lock for some time so we can later read the expiry time
        lock.lock(1000, TimeUnit.MILLISECONDS);

        // --- Check that mbean gets created at all
        holder.assertMBeanExistEventually("ILock", lock.getName());

        // --- Check remaining lease time
        final long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");
        // The remaining lease time has to be in the given range from the originally set value
        assertTrue(remainingLeaseTime > 500);
        assertTrue(remainingLeaseTime < 1000);

        // --- Check number of times locked (locked twice now)
        lock.lock(1000, TimeUnit.MILLISECONDS);
        final int lockCount = (Integer) holder.getMBeanAttribute("ILock", lock.getName(), "lockCount");
        assertEquals(2, lockCount);

        // --- Force unlock
        holder.invokeMBeanOperation("ILock", lock.getName(), "forceUnlock", null, null);
        assertTrue(!lock.isLocked());
    }
}