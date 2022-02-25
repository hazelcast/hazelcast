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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.Accessors.getNode;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationParkerImplTest extends HazelcastTestSupport {

    @Test
    public void testAwaitQueueCount_shouldNotExceedBlockedThreadCount() {
        final HazelcastInstance hz = createHazelcastInstance();
        NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
        OperationParkerImpl waitNotifyService = (OperationParkerImpl) nodeEngine.getOperationParker();

        final int keyCount = 1000;
        int nThreads = 4;
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++) {
            new Thread(new LockWaitAndUnlockTask(hz, keyCount, latch)).start();
        }

        while (latch.getCount() > 0) {
            LockSupport.parkNanos(1);
            int awaitQueueCount = waitNotifyService.getParkQueueCount();
            Assert.assertTrue(
                    "Await queue count should be smaller than total number of threads: " + awaitQueueCount + " VS "
                            + nThreads, awaitQueueCount < nThreads);
        }
    }

    private static class LockWaitAndUnlockTask implements Runnable {
        private final HazelcastInstance hz;
        private final int keyCount;
        private final CountDownLatch latch;

        LockWaitAndUnlockTask(HazelcastInstance hz, int keyCount, CountDownLatch latch) {
            this.hz = hz;
            this.keyCount = keyCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = 0; i < keyCount; i++) {
                IMap<Object, Object> map = hz.getMap("map");
                try {
                    String key = "key" + i;
                    map.lock(key);
                    LockSupport.parkNanos(1);
                    map.unlock(key);
                } catch (HazelcastInstanceNotActiveException ignored) {
                }
            }
            latch.countDown();
        }
    }
}
