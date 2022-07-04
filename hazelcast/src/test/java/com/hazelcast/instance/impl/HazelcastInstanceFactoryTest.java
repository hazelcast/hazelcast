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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestNodeContext;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static classloading.ThreadLeakTestUtils.assertHazelcastThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class HazelcastInstanceFactoryTest extends HazelcastTestSupport {

    private static Set<Thread> oldThreads;

    private HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void getThread() {
        oldThreads = getThreads();
    }

    @After
    public void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @AfterClass
    public static void cleanUp() {
        HazelcastInstanceFactory.terminateAll();

        assertHazelcastThreadShutdown(oldThreads);
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

            assertClusterSizeEventually(3, instance1, instance2, instance3);
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

            assertClusterSizeEventually(2, instance21, instance22);
            assertClusterSizeEventually(3, instance11, instance12, instance13);
        } finally {
            instanceFactory1.terminateAll();
            instanceFactory2.terminateAll();
        }
    }

    @Test
    public void test_NewInstance_configLoaded() {
        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(null);

        assertNotNull(hazelcastInstance.getConfig());
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
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

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
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

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
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

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
                }).when(nodeExtension).beforeShutdown(true);
                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
        try {
            hazelcastInstance.getLifecycleService().terminate();
        } catch (ExpectedRuntimeException expected) {
            hazelcastInstance.getLifecycleService().terminate();
            throw expected;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_NewInstance_terminateInstance_afterNodeStart() throws Exception {
        NodeContext context = new TestNodeContext() {
            @Override
            public NodeExtension createNodeExtension(final Node node) {
                NodeExtension nodeExtension = super.createNodeExtension(node);

                doAnswer(invocation -> {
                    node.hazelcastInstance.shutdown();
                    return null;
                }).when(nodeExtension).afterStart();

                return nodeExtension;
            }
        };

        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config, randomString(), context);
    }

    @Test
    public void mobyNameGeneratedIfPropertyEnabled() {
        Config config = new Config();
        config.getProperties().put(ClusterProperty.MOBY_NAMING_ENABLED.getName(), "true");

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config);
        String name = hazelcastInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }

    @Test
    public void fixedNameGeneratedIfPropertyDisabled() {
        Config config = new Config();
        config.getProperties().put(ClusterProperty.MOBY_NAMING_ENABLED.getName(), "false");

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config);
        String name = hazelcastInstance.getName();
        assertNotNull(name);
        assertContains(name, "_hzInstance_");
    }

    @Test
    public void fixedNameGeneratedIfPropertyNotDefined() {
        Config config = new Config();

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config);
        String name = hazelcastInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }

    @Test
    public void mobyNameGeneratedIfSystemPropertyEnabled() {
        Config config = new Config();
        ClusterProperty.MOBY_NAMING_ENABLED.setSystemProperty("true");

        hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config);
        String name = hazelcastInstance.getName();
        assertNotNull(name);
        assertNotContains(name, "_hzInstance_");
    }
}
