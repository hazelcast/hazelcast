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

package com.hazelcast.client.scheduledexecutor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceBasicTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientScheduledExecutorServiceBasicTest extends ScheduledExecutorServiceBasicTest {

    private TestHazelcastFactory factory;

    @After
    public void teardown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    @Override
    protected HazelcastInstance[] createClusterWithCount(int count) {
        return createClusterWithCount(count, new Config());
    }

    @Override
    protected HazelcastInstance[] createClusterWithCount(int count, Config config) {
        factory = new TestHazelcastFactory();
        HazelcastInstance[] instances = factory.newInstances(config, count);
        waitAllForSafeState(instances);
        return instances;
    }

    @Override
    public IScheduledExecutorService getScheduledExecutor(HazelcastInstance[] instances, String name) {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return factory.newHazelcastClient(config).getScheduledExecutorService(name);
    }

    @Test
    @Ignore("Never supported feature")
    @Override
    public void schedule_testPartitionLostEvent_withDurabilityCount() {
    }

    @Test
    @Ignore("Never supported feature")
    @Override
    public void schedule_testPartitionLostEvent_withMaxBackupCount() {
    }

    @Test
    @Ignore("Never supported feature")
    @Override
    public void scheduleOnMember_testMemberLostEvent() {
    }

    @Test
    @Ignore("Only implemented for server-side runs")
    @Override
    public void scheduledAtFixedRate_generates_statistics_when_stats_enabled() {
    }

    @Test
    @Ignore("Only implemented for server-side runs")
    @Override
    public void scheduledAtFixedRate_does_not_generate_statistics_when_stats_disabled() {
        super.scheduledAtFixedRate_does_not_generate_statistics_when_stats_disabled();
    }
}
