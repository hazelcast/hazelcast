package com.hazelcast.concurrent.semaphore;

import com.hazelcast.config.Config;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class})
public class SemaphoreQuorumWriteTest extends AbstractSemaphoreQuorumTest {

    @Parameterized.Parameter
    public static QuorumType quorumType;

    @Parameterized.Parameters(name = "classLoaderType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{QuorumType.WRITE}, {QuorumType.READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    // init(permits)
    // acquire()
    // acquire(permits)
    // drainPermits()
    // reducePermits(reduction)
    // release()
    // release(permits)
    // tryAcquire()
    // tryAcquire(permits)
    // tryAcquire(timout, timeunits)
    // tryAcquire(permits, timout, timeunits)

    @Test
    public void init_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).init(10);
    }

    @Test(expected = QuorumException.class)
    public void init_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).init(10);
    }

    @Test
    public void acquire_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).acquire();
    }

    @Test(expected = QuorumException.class)
    public void acquire_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).acquire();
    }

    @Test
    public void acquirePermits_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
        semaphore(0).acquire(2);
    }

    @Test(expected = QuorumException.class)
    public void acquirePermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).acquire(2);
    }

    @Test
    public void drainPermits_successful_whenQuorumSize_met() throws InterruptedException {
        int drained = 0;
        try {
            drained = semaphore(0).drainPermits();
        } finally {
            semaphore(0).release(drained);
        }
    }

    @Test(expected = QuorumException.class)
    public void drainPermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).drainPermits();
    }

    @Test
    public void reducePermits_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).reducePermits(1);
    }

    @Test(expected = QuorumException.class)
    public void reducePermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).reducePermits(1);
    }

    @Test
    public void release_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
    }

    @Test(expected = QuorumException.class)
    public void release_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).release();
    }

    @Test
    public void releasePermits_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
    }

    @Test(expected = QuorumException.class)
    public void releasePermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).release(2);
    }

    @Test
    public void tryAcquire_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).tryAcquire();
    }

    @Test(expected = QuorumException.class)
    public void tryAcquire_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire();
    }

    @Test
    public void tryAcquirePermits_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
        semaphore(0).tryAcquire(2);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquirePermits_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire(2);
    }

    @Test
    public void tryAcquireTimeout_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release();
        semaphore(0).tryAcquire(10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquireTimeout_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire(10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void tryAcquirePermitsTimeout_successful_whenQuorumSize_met() throws InterruptedException {
        semaphore(0).release(2);
        semaphore(0).tryAcquire(2, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = QuorumException.class)
    public void tryAcquirePermitsTimeout_successful_whenQuorumSize_notMet() throws InterruptedException {
        semaphore(3).tryAcquire(2, 10, TimeUnit.MILLISECONDS);
    }

    protected ISemaphore semaphore(int index) {
        return semaphore(index, quorumType);
    }

}
