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

package com.hazelcast.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.internal.cluster.impl.operations.GossipHeartbeatOp;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Test;

import static com.hazelcast.spi.properties.ClusterProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;

public class GossipTest extends HazelcastTestSupport {

    @Test
    public void name() {
        Config config = smallInstanceConfig().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "5");

        int nodeCount = 50;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            factory.newHazelcastInstance(config);
        }
        assertClusterSizeEventually(nodeCount, factory.getAllHazelcastInstances());
        GossipHeartbeatOp.ENABLE.set(true);
        int seconds = 30;
        sleepSeconds(seconds);
        GossipHeartbeatOp.ENABLE.set(false);

        System.out.println(String.format("----> Total number of heartbeats in %d seconds is %d",
                seconds, GossipHeartbeatOp.INSTANCE_COUNTER.get()));
        sleepSeconds(100);
    }
}
