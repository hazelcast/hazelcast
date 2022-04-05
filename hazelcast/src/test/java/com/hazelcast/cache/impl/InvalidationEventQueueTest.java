/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.impl;

import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.nearcache.impl.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvalidationEventQueueTest {

    private static final int WORKER_COUNT = 10;
    private static final int ITEM_COUNT_PER_WORKER = 100;

    @Test
    public void itemsShouldBeOfferedCorrectly() throws InterruptedException, ExecutionException, TimeoutException {
        final InvalidationQueue queue = new InvalidationQueue();
        List<Future> futureList = new ArrayList<Future>(WORKER_COUNT);

        for (int i = 0; i < WORKER_COUNT; i++) {
            Future future = spawn(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < ITEM_COUNT_PER_WORKER; i++) {
                        queue.offer(newInvalidation());
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
        final InvalidationQueue queue = new InvalidationQueue();
        List<Future> futureList = new ArrayList<Future>(WORKER_COUNT);

        for (int i = 0; i < WORKER_COUNT * ITEM_COUNT_PER_WORKER; i++) {
            queue.offer(newInvalidation());
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
        new InvalidationQueue().add(newInvalidation());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeOperationIsNotSupported() {
        new InvalidationQueue().remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeWithSpecifiedElementOperationIsNotSupported() {
        new InvalidationQueue().remove(newInvalidation());
    }

    protected SingleNearCacheInvalidation newInvalidation() {
        return new SingleNearCacheInvalidation(new HeapData(), "name",
                UUID.randomUUID(), UUID.randomUUID(), 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addAllOperationIsNotSupported() {
        new InvalidationQueue().addAll(new ArrayList<SingleNearCacheInvalidation>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeAllOperationIsNotSupported() {
        new InvalidationQueue().removeAll(new ArrayList<SingleNearCacheInvalidation>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void retainAllOperationIsNotSupported() {
        new InvalidationQueue().retainAll(new ArrayList<SingleNearCacheInvalidation>());
    }
}
