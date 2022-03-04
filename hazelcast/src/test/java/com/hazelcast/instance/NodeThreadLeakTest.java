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

package com.hazelcast.instance;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static classloading.ThreadLeakTestUtils.assertHazelcastThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests that an exception in the {@link Node} and {@link NodeEngineImpl} constructor leads to properly finished services.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeThreadLeakTest extends HazelcastTestSupport {

    private static final HazelcastException HAZELCAST_EXCEPTION
            = new HazelcastException("Test exception - Emulates service failure.");
    private static final String RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG = "%s should be created before throwing exception to check resource cleanup";
    private static final int TEST_TIMEOUT_IN_MILLIS = 30_000;

    @Test(timeout = TEST_TIMEOUT_IN_MILLIS)
    public void testLeakWhenCreatingConnectionManager() throws Exception {
        testFailingHazelcastCreation(new DefaultNodeContext() {
            @Override
            public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
                assertThat(node.getNodeEngine())
                        .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "Node.nodeEngine")
                        .isNotNull();
                super.createServer(node, registry, addressRegistry);
                throw HAZELCAST_EXCEPTION;
            }
        });
    }

    @Test(timeout = TEST_TIMEOUT_IN_MILLIS)
    public void testFailingInNodeEngineImplConstructor() throws Exception {
        testFailingHazelcastCreation(new DefaultNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                return new DefaultNodeExtension(node) {
                    @Override
                    public <T> T createService(Class<T> clazz, Object... params) {
                        NodeEngineImpl nodeEngine = (NodeEngineImpl) params[0];
                        assertNodeEnginePartiallyCreated(nodeEngine);
                        throw HAZELCAST_EXCEPTION;
                    }
                };
            }
        });
    }

    private static void assertNodeEnginePartiallyCreated(NodeEngineImpl nodeEngine) {
        assertThat(nodeEngine.getExecutionService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.executionService")
                .isNotNull();
        assertThat(nodeEngine.getEventService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.eventService")
                .isNotNull();
        assertThat(nodeEngine.getOperationParker())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.operationParker")
                .isNotNull();
        assertThat(nodeEngine.getOperationService())
                .withFailMessage(RESOURCE_SHOULD_BE_CREATED_BEFORE_TEST_MSG, "NodeEngineImpl.operationService")
                .isNotNull();
    }

    private void testFailingHazelcastCreation(DefaultNodeContext nodeContext) {

        Set<Thread> threads = getThreads();
        try {
            Config config = new Config();
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

            HazelcastInstanceFactory.newHazelcastInstance(
                    config,
                    config.getInstanceName(),
                    nodeContext
            );
            fail("Starting the member should have failed");
        } catch (HazelcastException expected) {
            ignore(expected);
        }

        assertHazelcastThreadShutdown("There are still Hazelcast threads running after failed service initialization!", threads);
    }
}
