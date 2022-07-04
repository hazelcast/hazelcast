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

package com.hazelcast.instance.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeExtensionTest extends HazelcastTestSupport {

    private HazelcastInstanceImpl hazelcastInstance;

    @After
    public void cleanup() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    public void verifyMethods() throws Exception {
        TestNodeContext nodeContext = new TestNodeContext();
        NodeExtension nodeExtension = nodeContext.getNodeExtension();

        hazelcastInstance = new HazelcastInstanceImpl(randomName(), getConfig(), nodeContext);

        InOrder inOrder = inOrder(nodeExtension);

        inOrder.verify(nodeExtension, times(1)).printNodeInfo();
        inOrder.verify(nodeExtension, times(1)).beforeStart();
        inOrder.verify(nodeExtension, times(1)).createSerializationService();
        inOrder.verify(nodeExtension, times(1)).createExtensionServices();
        inOrder.verify(nodeExtension, times(1)).beforeJoin();
        inOrder.verify(nodeExtension, times(1)).afterStart();

        hazelcastInstance.shutdown();
        inOrder.verify(nodeExtension, times(1)).beforeShutdown(false);
        inOrder.verify(nodeExtension, times(1)).shutdown();
    }

    protected Config getConfig() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        return config;
    }
}
