package com.hazelcast.jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LockMBeanTest extends HazelcastTestSupport {

    private JmxTestDataHolder holder = new JmxTestDataHolder();
    private ILock lock = holder.getHz().getLock("lock");

    @Test
    public void testMbeanExists_whenLockExists() throws Exception {
        // --- Check that mbean gets created at all
        holder.assertMBeanExistEventually("ILock", lock.getName());
    }

    @Test
    public void testLockCountIsTwo_whenLockedTwice() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);
        lock.lock(1000, TimeUnit.MILLISECONDS);

        // --- Check number of times locked (locked twice now)
        int lockCount = (Integer) holder.getMBeanAttribute("ILock", lock.getName(), "lockCount");

        assertEquals(2, lockCount);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        // --- Check remaining lease time
        long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");

        // The remaining lease time has to be in the given range from the originally set value
        assertTrue("Lease time: " + remainingLeaseTime, remainingLeaseTime > 0 && remainingLeaseTime < 1000);
    }

    @Test
    public void testIsNotLocked_whenMBeanForceUnlocked() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        holder.invokeMBeanOperation("ILock", lock.getName(), "forceUnlock", null, null);

        assertTrue("Lock is locked", ! lock.isLocked() );
    }
}
