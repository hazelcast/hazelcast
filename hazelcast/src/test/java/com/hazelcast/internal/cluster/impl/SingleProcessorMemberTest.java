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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.spi.properties.ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT;
import static com.hazelcast.test.OverridePropertyRule.clear;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleProcessorMemberTest extends HazelcastTestSupport {

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(1);

    @Rule
    public OverridePropertyRule gracefulShutdownTimeoutRule = clear(GRACEFUL_SHUTDOWN_MAX_WAIT.getName());

    @Test
    public void shutdownMembersConcurrently() throws Exception {
        shutdownMembersConcurrently(true);
    }

    @Test
    public void shutdownMembersConcurrently_withoutPartitionsInitialized() throws Exception {
        shutdownMembersConcurrently(false);
    }

    private void shutdownMembersConcurrently(boolean initializePartitions) throws Exception {
        // setting a very graceful shutdown high timeout value
        // to guarantee instance.shutdown() not to timeout
        gracefulShutdownTimeoutRule.setOrClearProperty(Integer.toString(Integer.MAX_VALUE));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(new Config(), 4);
        assertClusterSizeEventually(4, instances);
        if (initializePartitions) {
            warmUpPartitions(instances);
        }

        List<Future> futures = new ArrayList<>();
        for (final HazelcastInstance instance : instances) {
            Future future = spawn(instance::shutdown);
            futures.add(future);
        }

        for (Future future : futures) {
            assertCompletesEventually(future);
            future.get();
        }
    }
}
