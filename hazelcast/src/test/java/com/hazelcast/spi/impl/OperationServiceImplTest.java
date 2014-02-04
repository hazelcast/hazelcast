package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class OperationServiceImplTest extends HazelcastTestSupport {

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutSingleMember() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();
        final IQueue<Object> q = hz.getQueue("queue");

         for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz);
    }

    //there was a memory leak caused by the invocation not releasing the backup registration when there is a timeout.
    @Test
    public void testTimeoutWithMultiMemberCluster() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        final IQueue<Object> q = hz1.getQueue("queue");

        for (int k = 0; k < 1000; k++) {
            Object response = q.poll(1, TimeUnit.MILLISECONDS);
            assertNull(response);
        }

        assertNoLitterInOpService(hz1);
        assertNoLitterInOpService(hz2);
    }

    @Test
    public void testAsyncOpsSingleMember() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();
        final IMap<Object, Object> map = hz.getMap("test");

        final int count = 1500;
        for (int i = 0; i < count; i++) {
            map.putAsync(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(count, map.size());
            }
        });

        assertNoLitterInOpService(hz);
    }

    @Test
    public void testAsyncOpsMultiMember() throws InterruptedException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz2, hz);

        final IMap<Object, Object> map = hz.getMap("test");
        final IMap<Object, Object> map2 = hz2.getMap("test");

        final int count = 2000;
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                map.putAsync(i, i);
            } else {
                map2.putAsync(i, i);
            }
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(count, map.size());
                assertEquals(count, map2.size());
            }
        });

        assertNoLitterInOpService(hz);
        assertNoLitterInOpService(hz2);
    }

    private void assertNoLitterInOpService(HazelcastInstance hz) {
        final OperationServiceImpl operationService = (OperationServiceImpl) getNode(hz).nodeEngine.getOperationService();

        //we need to do this with an assertTrueEventually because it can happen that system calls are being send
        //and this leads to the maps not being empty. But eventually they will be empty at some moment in time.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("backup calls should be empty", 0, operationService.backupCalls.size());
                assertEquals("remote calls should be empty", 0, operationService.remoteCalls.size());
            }
        });
    }
}
