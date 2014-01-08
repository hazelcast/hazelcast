package com.hazelcast.spi.impl;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Stresses the OperationService to make sure that there are no memory leaks.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class InvocationMemoryLeakTest extends HazelcastTestSupport {

    public static final int ITERATION_COUNT = 100 * 1000 * 1000;

    @BeforeClass
    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void localNoBackups() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstanceFactory(1).newHazelcastInstance();
        IAtomicLong counter = hz.getAtomicLong("singleMemberReadonly");

        for (int k = 0; k < ITERATION_COUNT; k++) {
            counter.get();
            if (k % 10000 == 0) {
                System.out.println("at " + k);
            }
        }

        assertNoMoreRegisteredInvocations(hz);
    }

    @Test
    public void localBackupAwareOperation() throws InterruptedException {
        HazelcastInstance hz = createHazelcastInstanceFactory(1).newHazelcastInstance();
        IAtomicLong counter = hz.getAtomicLong("singleMemberReadonly");

        for (int k = 0; k < ITERATION_COUNT; k++) {
            counter.set(k);
            if (k % 10000 == 0) {
                System.out.println("at " + k);
            }
        }

        assertNoMoreRegisteredInvocations(hz);
    }

    @Test
    public void distributedBackupAwareOperation() throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[0];

        warmUpPartitions(instances);

        IAtomicLong[] counters = new IAtomicLong[100];
        for (int k = 0; k < counters.length; k++) {
            counters[k] = local.getAtomicLong(UUID.randomUUID().toString());
        }

        Random random = new Random();
        for (int k = 0; k < ITERATION_COUNT; k++) {
            IAtomicLong counter = getCounter(counters, random);
            counter.set(k);
            if (k % 10000 == 0) {
                System.out.println("at " + k);
            }
        }

        assertNoMoreRegisteredInvocations(local);
        assertNoMoreRegisteredInvocations(remote);
    }

    @Test
    public void distributedNonBackupAwareOperation() throws InterruptedException {
        HazelcastInstance[] instances = createHazelcastInstanceFactory(2).newInstances();
        HazelcastInstance local = instances[0];
        HazelcastInstance remote = instances[0];

        warmUpPartitions(instances);

        IAtomicLong[] counters = new IAtomicLong[100];
        for (int k = 0; k < counters.length; k++) {
            counters[k] = local.getAtomicLong(UUID.randomUUID().toString());
        }

        Random random = new Random();
        for (int k = 0; k < ITERATION_COUNT; k++) {
            IAtomicLong counter = getCounter(counters, random);
            counter.get();
            if (k % 10000 == 0) {
                System.out.println("at " + k);
            }
        }

        assertNoMoreRegisteredInvocations(local);
        assertNoMoreRegisteredInvocations(remote);
    }


    private IAtomicLong getCounter(IAtomicLong[] counters, Random random) {
        int r = random.nextInt(counters.length);
        return counters[r % counters.length];
    }

    private void assertNoMoreRegisteredInvocations(HazelcastInstance local) {
        BasicOperationService basicOperationService = (BasicOperationService) getNode(local).nodeEngine.getOperationService();
        assertEquals(0, basicOperationService.getRegisteredInvocationCount());
    }
}