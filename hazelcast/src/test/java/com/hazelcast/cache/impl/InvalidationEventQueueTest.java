package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.CacheEventHandler.InvalidationEventQueue;
import com.hazelcast.cache.impl.client.CacheSingleInvalidationMessage;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvalidationEventQueueTest {

    @Test
    public void itemsShouldBeOfferedCorrectly() throws InterruptedException, ExecutionException, TimeoutException {
        final int WORKER_COUNT = 10;
        final int ITEM_COUNT_PER_WORKER = 100;
        final InvalidationEventQueue queue = new InvalidationEventQueue();
        List<Future> futureList = new ArrayList<Future>(WORKER_COUNT);

        for (int i = 0; i < WORKER_COUNT; i++) {
            Future future = spawn(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < ITEM_COUNT_PER_WORKER; i++) {
                        queue.offer(new CacheSingleInvalidationMessage(null, null, null));
                    }
                }
            });
            futureList.add(future);
        }

        for (Future future : futureList) {
            future.get(30, TimeUnit.SECONDS);
        }

        assertEquals(WORKER_COUNT * ITEM_COUNT_PER_WORKER, queue.size());
    }

    @Test
    public void itemsShouldBePolledCorrectly() throws InterruptedException, ExecutionException, TimeoutException {
        final int WORKER_COUNT = 10;
        final int ITEM_COUNT_PER_WORKER = 100;
        final InvalidationEventQueue queue = new InvalidationEventQueue();
        List<Future> futureList = new ArrayList<Future>(WORKER_COUNT);

        for (int i = 0; i < WORKER_COUNT * ITEM_COUNT_PER_WORKER; i++) {
            queue.offer(new CacheSingleInvalidationMessage(null, null, null));
        }

        for (int i = 0; i < WORKER_COUNT; i++) {
            Future future = spawn(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < ITEM_COUNT_PER_WORKER; i++) {
                        queue.poll();
                    }
                }
            });
            futureList.add(future);
        }

        for (Future future : futureList) {
            future.get(30, TimeUnit.SECONDS);
        }

        assertEquals(0, queue.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addOperationIsNotSupported() {
        new InvalidationEventQueue().add(new CacheSingleInvalidationMessage(null, null, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeOperationIsNotSupported() {
        new InvalidationEventQueue().remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeWithSpecifiedElementOperationIsNotSupported() {
        new InvalidationEventQueue().remove(new CacheSingleInvalidationMessage(null, null, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addAllOperationIsNotSupported() {
        new InvalidationEventQueue().addAll(new ArrayList<CacheSingleInvalidationMessage>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeAllOperationIsNotSupported() {
        new InvalidationEventQueue().removeAll(new ArrayList<CacheSingleInvalidationMessage>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void retainAllOperationIsNotSupported() {
        new InvalidationEventQueue().retainAll(new ArrayList<CacheSingleInvalidationMessage>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clearOperationIsNotSupported() {
        new InvalidationEventQueue().clear();
    }

}
