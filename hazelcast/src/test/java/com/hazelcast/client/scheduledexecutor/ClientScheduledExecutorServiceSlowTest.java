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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceSlowTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientScheduledExecutorServiceSlowTest extends ScheduledExecutorServiceSlowTest {

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
        return factory.newHazelcastClient().getScheduledExecutorService(name);
    }
}
