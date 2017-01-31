/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
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
@Category(QuickTest.class)
public class HazelcastInstanceFactoryTest extends HazelcastTestSupport {

    private HazelcastInstance hazelcastInstance;

    @After
    public void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @AfterClass
    public static void cleanUp() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(HazelcastInstanceFactory.class);
    }

    @Test
    public void testTestHazelcastInstanceFactory() {
        TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory();
        try {
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
        } finally {
            instanceFactory.terminateAll();
        }
    }

    @Test
    public void testTestHazelcastInstanceFactory_withTwoFactories() {
        TestHazelcastInstanceFactory instanceFactory1 = new TestHazelcastInstanceFactory();
        TestHazelcastInstanceFactory instanceFactory2 = new TestHazelcastInstanceFactory();
        try {
            final HazelcastInstance instance11 = instanceFactory1.newHazelcastInstance();
            final HazelcastInstance instance12 = instanceFactory1.newHazelcastInstance();
            final HazelcastInstance instance13 = instanceFactory1.newHazelcastInstance();

            final HazelcastInstance instance21 = instanceFactory2.newHazelcastInstance();
            final HazelcastInstance instance22 = instanceFactory2.newHazelcastInstance();

            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(2, instance21.getCluster().getMembers().size());
                    assertEquals(2, instance22.getCluster().getMembers().size());
                }
            });
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(3, instance11.getCluster().getMembers().size());
                    assertEquals(3, instance12.getCluster().getMembers().size());
                    assertEquals(3, instance13.getCluster().getMembers().size());
                }
            });
        } finally {
            instanceFactory1.terminateAll();
            instanceFactory2.terminateAll();
        }
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
        try {
            hazelcastInstance.getLifecycleService().terminate();
        } catch (ExpectedRuntimeException expected) {
            hazelcastInstance.getLifecycleService().terminate();
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
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

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
    }
}
