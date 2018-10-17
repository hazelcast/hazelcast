/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.spi.ExecutionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NodeMetricsTest extends HazelcastInstanceMetricsIntegrationTest {

    @Test
    public void clusterServiceStats() {
        assertHasStats(9, "cluster");
        assertHasStats(6, "cluster.clock");
        assertHasStats(1, "cluster.heartbeat");
    }

    @Test
    public void proxyServiceStats() {
        assertHasStats(3, "proxy");
    }

    @Test
    public void memoryStats() {
        assertHasStats(12, "memory");
    }

    @Test
    public void gcStats() {
        assertHasStats(6, "gc");
    }

    @Test
    public void runtimeStats() {
        assertHasStats(6, "runtime");
    }

    @Test
    public void classloadingStats() {
        assertHasStats(3, "classloading");
    }

    @Test
    public void osStats() {
        assertHasStats(11, "os");
    }

    @Test
    public void threadStats() {
        assertHasStats(4, "thread");
    }

    @Test
    public void userHomeStats() {
        assertHasStats(4, "file.partition", "user.home");
    }

    @Test
    public void partitionServiceStats() {
        assertHasStats(10, "partitions");
    }

    @Test
    public void transactionServiceStats() {
        assertHasStats(3, "transactions");
    }

    @Test
    public void clientServiceStats() {
        assertHasStats(2, "client.endpoint");
    }

    @Test
    public void operationsServiceStats() {
        assertHasStats(23, "operation");
        assertHasStats(6, "operation.invocations");
        assertHasStats(2, "operation.parker");
        assertHasStats(6, "operation.responses");
    }

    @Test
    public void executorServiceStats() {
        assertHasStats(5, "internal-executor", ExecutionService.ASYNC_EXECUTOR);
        assertHasStats(5, "internal-executor", ExecutionService.CLIENT_EXECUTOR);
        assertHasStats(5, "internal-executor", ExecutionService.SCHEDULED_EXECUTOR);
        assertHasStats(5, "internal-executor", ExecutionService.QUERY_EXECUTOR);
        assertHasStats(5, "internal-executor", ExecutionService.SYSTEM_EXECUTOR);
    }

}
