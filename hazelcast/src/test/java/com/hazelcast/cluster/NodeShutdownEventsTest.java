/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NodeShutdownEventsTest {

    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    /**
     * When a node fails due to a join time out, it will be shutdowned.
     * In that scenario we are expecting lifecycle events (SHUTTING_DOWN & SHUTDOWN)
     * to be fired locally.
     */
    @Test
    public void testNodeShutdown_firesLifecycleEvents_afterJoinFailure() throws Exception {
        // Only expecting SHUTTING_DOWN & SHUTDOWN events so latch count should be 2.
        final CountDownLatch shutdownEventCount = new CountDownLatch(2);

        final Config config1 = new Config();

        final Config config2 = new Config();
        // force join failure.
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "-100");
        // add lifecycle listener.
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

        final HazelcastInstance node1 = Hazelcast.newHazelcastInstance(config1);
        try {
            final HazelcastInstance node2 = Hazelcast.newHazelcastInstance(config2);
        } catch (IllegalStateException e) {
            // ignore IllegalStateException since we are only testing lifecyle events.
        }

        assertOpenEventually(shutdownEventCount);
    }


}
