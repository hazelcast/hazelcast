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

package com.hazelcast.internal.util.executor;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuickTest
@ParallelJVMTest
public class StripedExecutorTest {
    @Test
    public void throws_illegalArgumentException_whenThreadCount_isNotPositive() {
        assertThrows(IllegalArgumentException.class, () -> getStripedExecutor(0, 1));
    }

    @Test
    public void throws_illegalArgumentException_whenMaximumQueueCapacity_isNotPositive() {
        assertThrows(IllegalArgumentException.class, () -> getStripedExecutor(1, 0));
    }

    @Test
    public void total_worker_queue_size_equals_max_queue_capacity() {
        int threadCount = 5;
        int maximumQueueCapacity = 1000000;

        StripedExecutor executor = getStripedExecutor(threadCount, maximumQueueCapacity);

        assertEquals(maximumQueueCapacity, calculateWorkersTotalQueueCapacity(executor));
    }

    /** There was a bug that the metrics would increase every time they were queried, assert they return a constant value */
    @Test
    public void test_metrics_are_consistent() {
        StripedExecutor executor = getStripedExecutor(1, 1);

        assertEquals(executor.getWorkQueueSize(), executor.getWorkQueueSize());
        assertEquals(executor.processedCount(), executor.processedCount());
    }

    private static int calculateWorkersTotalQueueCapacity(StripedExecutor executor) {
        int totalQueueCapacity = 0;
        StripedExecutor.Worker[] workers = executor.getWorkers();
        for (StripedExecutor.Worker worker : workers) {
            totalQueueCapacity += worker.getQueueCapacity();
        }
        return totalQueueCapacity;
    }

    private StripedExecutor getStripedExecutor(int threadCount, int queueCapacity) {
        return new StripedExecutor(getLogger(getClass()), "", threadCount, queueCapacity);
    }
}
