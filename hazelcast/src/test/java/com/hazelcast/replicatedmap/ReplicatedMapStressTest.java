package com.hazelcast.replicatedmap;

import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


@Category(NightlyTest.class)
public class ReplicatedMapStressTest extends HazelcastTestSupport {
    @Test
    public void stressTestRemove() throws Exception {
        int noOfThreads = 50;
        final CountDownLatch latch = new CountDownLatch(noOfThreads);
        final AtomicInteger noOfFailures = new AtomicInteger();


        for (int i = 0; i < noOfThreads; i++) {
            new Thread() {
                @Override
                public void run() {
                    ReplicatedMapTest test = new ReplicatedMapTest();
                    try {
                        test.testRemoveBinaryDelayDefault();
                    } catch (Exception e) {
                        noOfFailures.incrementAndGet();
                    } catch (Error e) {
                        noOfFailures.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }
            }.start();
        }
        assertOpenEventually(latch, 60);
        assertEquals(0, noOfFailures.get());
    }
}
