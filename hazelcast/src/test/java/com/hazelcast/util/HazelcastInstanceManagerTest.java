/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastInstanceManagerTest extends HazelcastTestSupport {

    @Test
    public void testTestHazelcastInstanceFactory() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());
            }
        });

        instanceFactory.terminateAll();

    }

    @Test
    public void testTestHazelcastInstanceFactory_withTwoFactories() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        TestHazelcastInstanceFactory instanceFactory2 = new TestHazelcastInstanceFactory();
        final HazelcastInstance instance1_2 = instanceFactory2.newHazelcastInstance();
        final HazelcastInstance instance2_2 = instanceFactory2.newHazelcastInstance();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, instance1_2.getCluster().getMembers().size());
                assertEquals(2, instance2_2.getCluster().getMembers().size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());
            }
        });

        instanceFactory.terminateAll();
        instanceFactory2.terminateAll();

    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).beforeStart();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeJoin() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).beforeJoin();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_afterNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).afterStart();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failed_beforeNodeShutdown() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doAnswer(new Answer() {
                    final AtomicBoolean throwException = new AtomicBoolean(false);

                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        if (throwException.compareAndSet(false, true)) {
                            throw new ExpectedRuntimeException();
                        }
                        return null;
                    }
                }).when(nodeExtension).beforeShutdown();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstance instance = HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
        try {
            instance.getLifecycleService().terminate();
        } catch (ExpectedRuntimeException expected) {
            instance.getLifecycleService().terminate();
            throw expected;
        }
    }

    @Test(expected = ExpectedRuntimeException.class)
    public void test_NewInstance_failedAfterStartAndBeforeShutdown() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).afterStart();
                doThrow(new ExpectedRuntimeException()).when(nodeExtension).beforeShutdown();
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
    }

    @Test(expected = IllegalStateException.class)
    public void test_NewInstance_terminateInstance_afterNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(final Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);

                doAnswer(new Answer() {
                    @Override
                    public Object answer(InvocationOnMock invocation) throws Throwable {
                        node.hazelcastInstance.shutdown();
                        return null;
                    }
                }).when(nodeExtension).afterStart();

                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        HazelcastInstanceManager.newHazelcastInstance(config, randomString(), context);
    }
}
