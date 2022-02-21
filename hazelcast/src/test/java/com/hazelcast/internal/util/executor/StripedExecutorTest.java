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

package com.hazelcast.internal.util.executor;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StripedExecutorTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void throws_illegalArgumentException_whenThreadCount_isNotPositive() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        new StripedExecutor(getLogger(getClass()), "", 0, 0);
    }

    @Test
    public void throws_illegalArgumentException_whenMaximumQueueCapacity_isNotPositive() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        new StripedExecutor(getLogger(getClass()), "", 0, 0);
    }

    @Test
    public void total_worker_queue_size_equals_max_queue_capacity() throws Exception {
        int threadCount = 5;
        int maximumQueueCapacity = 1000000;

        StripedExecutor executor = new StripedExecutor(getLogger(getClass()), "",
                threadCount, maximumQueueCapacity);

        assertEquals(maximumQueueCapacity, calculateWorkersTotalQueueCapacity(executor));
    }

    private static int calculateWorkersTotalQueueCapacity(StripedExecutor executor) {
        int totalQueueCapacity = 0;
        StripedExecutor.Worker[] workers = executor.getWorkers();
        for (StripedExecutor.Worker worker : workers) {
            totalQueueCapacity += worker.getQueueCapacity();
        }
        return totalQueueCapacity;
    }
}
