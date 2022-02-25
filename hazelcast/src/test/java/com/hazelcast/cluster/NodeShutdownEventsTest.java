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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeShutdownEventsTest extends HazelcastTestSupport {

    /**
     * When a node fails during join, it will be shut down.
     * In that scenario we are expecting lifecycle events (SHUTTING_DOWN & SHUTDOWN)
     * to be fired locally.
     */
    @Test
    public void testNodeShutdown_firesLifecycleEvents_afterJoinFailure() {
        // Only expecting SHUTTING_DOWN & SHUTDOWN events so latch count should be 2.
        final CountDownLatch shutdownEventCount = new CountDownLatch(2);

        // Having different partition counts is cause of the join failure.
        Config config1 = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "111");

        Config config2 = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "222");

        final ListenerConfig listenerConfig = new ListenerConfig();
        listenerConfig.setImplementation(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                // Only expecting SHUTTING_DOWN & SHUTDOWN.
                if (LifecycleEvent.LifecycleState.SHUTTING_DOWN.equals(event.getState())
                        || LifecycleEvent.LifecycleState.SHUTDOWN.equals(event.getState())) {
                    shutdownEventCount.countDown();
                }
            }
        });
        config2.addListenerConfig(listenerConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config1);

        try {
            factory.newHazelcastInstance(config2);
            fail("Second node should fail during join");
        } catch (IllegalStateException e) {
            // ignore IllegalStateException since we are only testing lifecycle events.
        }

        assertOpenEventually(shutdownEventCount);
    }
}
