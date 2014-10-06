package com.hazelcast.jmx;

import com.hazelcast.core.ILock;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class LockMBeanTest extends HazelcastTestSupport {

    private JmxTestDataHolder holder = new JmxTestDataHolder();
    private ILock lock = holder.getHz().getLock("lock");

    @Before
    public void ensureMBeanCreated() throws Exception {
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
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_remainingLeaseTimeCannotBeGreaterThanOriginal() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");
        assertFalse(remainingLeaseTime > 1000);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_mustBeLockedWhenHasRemainingLease() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        boolean isLocked = lock.isLockedByCurrentThread();
        long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");
        boolean hasRemainingLease = remainingLeaseTime > 0;
        assertTrue(isLocked || !hasRemainingLease);
    }


    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_eitherHasRemainingLeaseOrIsUnlocked() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");
        boolean hasLeaseRemaining = remainingLeaseTime > 0;
        boolean isLocked = lock.isLockedByCurrentThread();
        assertTrue((isLocked && hasLeaseRemaining) || !isLocked);
    }

    @Test
    public void testMBeanHasLeaseTime_whenLockedWithLeaseTime_mustHaveRemainingLeaseBeforeItExpires() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);
        long startTime = Clock.currentTimeMillis();

        long remainingLeaseTime = (Long) holder.getMBeanAttribute("ILock", lock.getName(), "remainingLeaseTime");
        long timePassed = Clock.currentTimeMillis() - startTime;
        boolean hasLeaseRemaining = remainingLeaseTime > 0;

        assertTrue(hasLeaseRemaining || timePassed >= 1000);
    }

    @Test
    public void testIsNotLocked_whenMBeanForceUnlocked() throws Exception {
        lock.lock(1000, TimeUnit.MILLISECONDS);

        holder.invokeMBeanOperation("ILock", lock.getName(), "forceUnlock", null, null);

        assertFalse("Lock is locked", lock.isLocked());
    }
}
