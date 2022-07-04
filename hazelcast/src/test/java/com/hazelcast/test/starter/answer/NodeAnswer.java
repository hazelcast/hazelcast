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

package com.hazelcast.test.starter.answer;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.mockito.invocation.InvocationOnMock;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyArgumentsIfNeeded;
import static org.mockito.Mockito.mock;

/**
 * Default {@link org.mockito.stubbing.Answer} to create a mock for a proxied
 * {@link Node}.
 * <p>
 * Usage:
 * <pre><code>
 *   Object delegate = HazelcastStarter.getNode(hz, classloader);
 *   mock(Node.class, new NodeAnswer(delegate);
 * </code></pre>
 */
public class NodeAnswer extends AbstractAnswer {

    public NodeAnswer(Object delegate) {
        super(delegate);
    }

    @Override
    Object answer(InvocationOnMock invocation, String methodName, Object[] arguments) throws Exception {
        if (arguments.length == 1 && methodName.equals("getLogger")) {
            return getLogger(arguments);
        } else if (arguments.length == 0 && methodName.equals("getClusterService")) {
            Object clusterService = invokeForMock(invocation);
            return mock(ClusterServiceImpl.class, new ClusterServiceAnswer(clusterService));
        } else if (arguments.length == 0 && methodName.equals("getPartitionService")) {
            Object partitionService = invokeForMock(invocation);
            return mock(InternalPartitionService.class, new PartitionServiceAnswer(partitionService));
        } else if (arguments.length == 0 && methodName.equals("getNodeEngine")) {
            Object nodeEngine = invokeForMock(invocation);
            return mock(NodeEngineImpl.class, new NodeEngineAnswer(nodeEngine));
        } else if (arguments.length == 1 && methodName.equals("getConnectionManager")) {
            arguments = proxyArgumentsIfNeeded(arguments, delegateClassloader);
            Object endpointManager = invokeForMock(invocation, arguments);
            return createMockForTargetClass(endpointManager, new FirewallingConnectionManagerAnswer(endpointManager));
        } else if (arguments.length == 0 && methodName.equals("getServer")) {
            Object server = invokeForMock(invocation);
            return createMockForTargetClass(server, new ServerAnswer(server));
        } else if (arguments.length == 0 && (methodName.startsWith("get") || methodName.startsWith("is"))) {
            return invoke(invocation);
        }
        throw new UnsupportedOperationException("Method is not implemented in NodeAnswer: " + methodName);
    }
}
