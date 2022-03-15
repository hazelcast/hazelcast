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

package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.function.Function;

public class CustomNodeExtensionTestInstanceFactory extends TestHazelcastInstanceFactory {
    private Function<Node, NodeExtension> nodeExtensionFn;

    public CustomNodeExtensionTestInstanceFactory(Function<Node, NodeExtension> nodeExtensionFn) {
        super();
        this.nodeExtensionFn = nodeExtensionFn;
    }

    @Override
    public HazelcastInstance newHazelcastInstance(Config config) {
        String instanceName = config != null ? config.getInstanceName() : null;
        NodeContext nodeContext;
        if (TestEnvironment.isMockNetwork()) {
            config = initOrCreateConfig(config);
            nodeContext = this.registry.createNodeContext(this.nextAddress(config.getNetworkConfig().getPort()));
        } else {
            nodeContext = new DefaultNodeContext();
        }
        return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName,
                new WanServiceMockingNodeContext(nodeContext, nodeExtensionFn));
    }
}
