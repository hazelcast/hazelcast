/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.root;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.worker.QueryFragmentScheduleCallback;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BlockingRootResultConsumerTest extends HazelcastTestSupport {
    @Test
    public void testConsumeAtMostOneBatch() {
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();

        List<Row> batch = Collections.singletonList(HeapRow.of(1));

        assertTrue(consumer.consume(batch, false));
        assertFalse(consumer.consume(batch, false));
    }

    @Test
    public void testIterator() {
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();

        Iterator<Row> iterator = consumer.iterator();
        assertSame(iterator, consumer.iterator());

        consumer.consume(Collections.singletonList(HeapRow.of(1)), true);

        assertTrue(iterator.hasNext());
        assertEquals(1, (int) iterator.next().get(0));

        assertFalse(iterator.hasNext());

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testIteration() {
        // Prepare batches that will be transferred.
        int batchCount = 10;
        int rowsPerBatch = 2;

        List<Row> allRows = new ArrayList<>();
        ArrayDeque<List<Row>> batches = new ArrayDeque<>();

        int valueCouner = 0;

        for (int i = 0; i < batchCount; i++) {
            List<Row> batch = new ArrayList<>();

            for (int j = 0; j < rowsPerBatch; j++) {
                HeapRow row = HeapRow.of(valueCouner++);

                allRows.add(row);
                batch.add(row);
            }

            batches.add(batch);
        }

        // Prepare consumer
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();
        AtomicInteger scheduleInvocationCount = new AtomicInteger();

        QueryFragmentScheduleCallback scheduleCallback = () -> {
            assertFalse(batches.isEmpty());

            List<Row> batch = batches.poll();

            // Push the next batch asynchronously.
            new Thread(() -> consumer.consume(batch, batches.isEmpty())).start();

            scheduleInvocationCount.incrementAndGet();

            return true;
        };

        consumer.setup(new QueryFragmentContext(Collections.emptyList(), scheduleCallback, null));

        // Start consuming.
        IteratorRunnable runnable = startConsuming(consumer);

        // Setup the first batch.
        assertFalse(batches.isEmpty());

        consumer.consume(batches.poll(), false);

        // Make sure that all rows are there eventually.
        runnable.awaitCompletion();

        assertEquals(batchCount - 1, scheduleInvocationCount.get());

        assertFalse(runnable.hasError());

        List<Row> rows = runnable.awaitRows(allRows.size());
        assertEquals(allRows, rows);
    }

    @Test
    public void testError() {
        // Prepare original batch.
        List<Row> batch = new ArrayList<>();

        batch.add(HeapRow.of(0));
        batch.add(HeapRow.of(1));

        // Prepare consumer.
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();
        AtomicInteger scheduleInvocationCount = new AtomicInteger();
        QueryException error = QueryException.error("Test");

        QueryFragmentScheduleCallback scheduleCallback = () -> {
            // Trigger an error asynchronously.
            new Thread(() -> consumer.onError(error)).start();

            scheduleInvocationCount.incrementAndGet();

            return true;
        };

        consumer.setup(new QueryFragmentContext(Collections.emptyList(), scheduleCallback, null));

        // Start consuming.
        IteratorRunnable runnable = startConsuming(consumer);

        // Setup the first batch.
        consumer.consume(batch, false);

        // Make sure that iteration is finished due to an error.
        runnable.awaitCompletion();

        assertEquals(1, scheduleInvocationCount.get());

        assertTrue(runnable.hasError());
        assertSame(error, runnable.getError());

        List<Row> rows = runnable.awaitRows(batch.size());
        assertEquals(batch, rows);
    }

    private static IteratorRunnable startConsuming(BlockingRootResultConsumer consumer) {
        IteratorRunnable runnable = new IteratorRunnable(consumer);

        Thread thread = new Thread(runnable);

        thread.setDaemon(true);

        thread.start();

        return runnable;
    }

    /**
     * Convenient runnable to fetch results in a separate thread.
     */
    private static class IteratorRunnable implements Runnable {

        private final BlockingRootResultConsumer consumer;
        private final LinkedBlockingQueue<Row> rows = new LinkedBlockingQueue<>();
        private final CountDownLatch completeLatch = new CountDownLatch(1);
        private volatile Exception error;

        private IteratorRunnable(BlockingRootResultConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                Iterator<Row> iterator = consumer.iterator();

                while (iterator.hasNext()) {
                    rows.add(iterator.next());
                }
            } catch (Exception e) {
                error = e;
            } finally {
                completeLatch.countDown();
            }
        }

        private QueryException getError() {
            assertTrue(error instanceof QueryException);

            return (QueryException) error;
        }

        private boolean hasError() {
            return error != null;
        }

        private List<Row> awaitRows(int count) {
            assertTrueEventually(() -> assertTrue(rows.size() >= count));

            List<Row> res = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                Row row = rows.poll();

                assertNotNull(row);

                res.add(row);
            }

            return res;
        }

        private void awaitCompletion() {
            assertOpenEventually(completeLatch);
        }
    }
}
