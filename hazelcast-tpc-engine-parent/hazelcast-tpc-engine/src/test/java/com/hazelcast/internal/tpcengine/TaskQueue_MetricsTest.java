/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaskQueue_MetricsTest {

    @Test
    public void test() {
        TaskQueue.Metrics metrics = new TaskQueue.Metrics();

        assertEquals(0, metrics.taskCsCount());
        metrics.incTaskCsCount();
        assertEquals(1, metrics.taskCsCount());

        assertEquals(0, metrics.cpuTimeNanos());
        metrics.incCpuTimeNanos(20);
        assertEquals(20, metrics.cpuTimeNanos());

        assertEquals(0, metrics.blockedCount());
        metrics.incBlockedCount();
        assertEquals(1, metrics.blockedCount());

        assertEquals(0, metrics.taskErrorCount());
        metrics.incTaskErrorCount();
        assertEquals(1, metrics.taskErrorCount());
    }
}
