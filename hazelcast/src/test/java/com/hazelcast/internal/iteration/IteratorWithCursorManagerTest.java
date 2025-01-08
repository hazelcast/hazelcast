/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.iteration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IteratorWithCursorManagerTest extends HazelcastTestSupport {
    private static final int THREAD_COUNT = 8;
    private IteratorWithCursorManager<Integer> iteratorWithCursorManager;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance();
        iteratorWithCursorManager = new IteratorWithCursorManager<>(getNodeEngineImpl(hz));
    }

    @Test
    public void testIteration() throws InterruptedException {
        ExecutorService executor = newFixedThreadPool(THREAD_COUNT);

        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int j = i;
            executor.execute(() -> {
                iterate(j);
                latch.countDown();
            });
        }
        latch.await();

        executor.shutdown();
    }

    @Test
    public void testIteratorIdSentTwice() {
        int pageSize = 100;
        int totalCount = 1000;

        UUID iteratorId = UuidUtil.newUnsecureUUID();
        List<Integer> data = new ArrayList<>(totalCount);
        for (int i = 0; i < totalCount; i++) {
            data.add(i);
        }
        iteratorWithCursorManager.createIterator(data.iterator(), iteratorId);
        List<Integer> items = iteratorWithCursorManager.iterate(iteratorId, pageSize).getPage();
        // client retries
        List<Integer> itemsRetry = iteratorWithCursorManager.iterate(iteratorId, pageSize).getPage();
        assertThat(items).containsExactlyInAnyOrderElementsOf(itemsRetry);
    }

    @Test
    public void testCursorIdSentTwice() {
        int pageSize = 100;
        int totalCount = 1000;

        UUID iteratorId = UuidUtil.newUnsecureUUID();
        List<Integer> data = new ArrayList<>(totalCount);
        for (int i = 0; i < totalCount; i++) {
            data.add(i);
        }
        iteratorWithCursorManager.createIterator(data.iterator(), iteratorId);
        UUID cursorId = iteratorWithCursorManager.iterate(iteratorId, pageSize).getCursorId();

        List<Integer> items = iteratorWithCursorManager.iterate(cursorId, pageSize).getPage();
        // client retries
        List<Integer> itemsRetry = iteratorWithCursorManager.iterate(cursorId, pageSize).getPage();

        assertThat(items).containsExactlyInAnyOrderElementsOf(itemsRetry);
    }

    private void iterate(int j) {
        int pageSize = 100;
        int totalCount = j * pageSize;
        List<Integer> iterated = new ArrayList<>(totalCount);

        List<Integer> data = new ArrayList<>(totalCount);
        for (int i = j * 1000; i < j * 1000 + totalCount; i++) {
            data.add(i);
        }
        UUID iteratorId = UuidUtil.newUnsecureUUID();
        iteratorWithCursorManager.createIterator(data.iterator(), iteratorId);

        assertThat(iteratorWithCursorManager.getIterators().keySet()).contains(iteratorId);
        assertThat(iteratorWithCursorManager.getCursorToIteratorId()).containsEntry(iteratorId, iteratorId);
        IterationResult<Integer> result;
        UUID cursorId = iteratorId;
        do {
            UUID previousCursorId = new UUID(cursorId.getMostSignificantBits(), cursorId.getLeastSignificantBits());
            result = iteratorWithCursorManager.iterate(cursorId, pageSize);
            cursorId = result.getCursorId();
            assertThat(cursorId).isNotEqualTo(previousCursorId);
            assertThat(result.getPage()).hasSizeLessThanOrEqualTo(pageSize);
            iterated.addAll(result.getPage());
            assertThat(iteratorWithCursorManager.getIterators().keySet()).contains(iteratorId);
        } while (!result.isEmpty());
        assertThat(iterated).containsExactlyElementsOf(data);
        iteratorWithCursorManager.cleanupIterator(iteratorId);
        assertThat(iteratorWithCursorManager.getIterators().keySet()).doesNotContain(iteratorId);
    }
}
