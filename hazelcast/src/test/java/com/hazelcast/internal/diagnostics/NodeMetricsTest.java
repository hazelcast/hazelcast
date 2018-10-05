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
        assertHasStatsEventually(16, "cluster.");
        assertHasStatsEventually(6, "cluster.clock");
    }

    @Test
    public void proxyServiceStats() {
        assertHasStatsEventually(3, "proxy.");
    }

    @Test
    public void memoryStats() {
        assertHasStatsEventually(12, "memory.");
    }

    @Test
    public void gcStats() {
        assertHasStatsEventually(6, "gc.");
    }

    @Test
    public void runtimeStats() {
        assertHasStatsEventually(6, "runtime.");
    }

    @Test
    public void classloadingStats() {
        assertHasStatsEventually(3, "classloading.");
    }

    @Test
    public void osStats() {
        assertHasStatsEventually(11, "os.");
    }

    @Test
    public void threadStats() {
        assertHasStatsEventually(4, "thread.");
    }

    @Test
    public void userHomeStats() {
        assertHasStatsEventually(4, "file.partition", "user.home");
    }

    @Test
    public void partitionServiceStats() {
        assertHasStatsEventually(10, "partitions.");
    }

    @Test
    public void transactionServiceStats() {
        assertHasStatsEventually(3, "transactions.");
    }

    @Test
    public void clientServiceStats() {
        assertHasStatsEventually(2, "client.endpoint.");
    }

    @Test
    public void operationsServiceStats() {
        assertHasStatsEventually(32, "operation.");
        assertHasStatsEventually(5, "operation.responses.");
        assertHasStatsEventually(3, "operation.invocations.");
        assertHasStatsEventually(2, "operation.parker.");
    }

    @Test
    public void executorServiceStats() {
        assertHasStatsEventually(5, "internal-executor", ExecutionService.ASYNC_EXECUTOR);
        assertHasStatsEventually(5, "internal-executor", ExecutionService.CLIENT_EXECUTOR);
        assertHasStatsEventually(5, "internal-executor", ExecutionService.SCHEDULED_EXECUTOR);
        assertHasStatsEventually(5, "internal-executor", ExecutionService.QUERY_EXECUTOR);
        assertHasStatsEventually(5, "internal-executor", ExecutionService.SYSTEM_EXECUTOR);
    }

}
