/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.partition.TestPartitionUtils;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class HazelcastStarterTest {

    private HazelcastInstance hz;

    @After
    public void tearDown() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @Test
    public void testMember() {
        hz = HazelcastStarter.newHazelcastInstance("3.7", false);

        HazelcastInstance bouncingInstance = null;
        try {
            for (int i = 1; i < 6; i++) {
                String version = "3.7." + i;
                System.out.println("Starting member " + version);
                bouncingInstance = HazelcastStarter.newHazelcastInstance(version);
                System.out.println("Stopping member " + version);
                bouncingInstance.shutdown();
            }
        } finally {
            if (bouncingInstance != null && bouncingInstance.getLifecycleService().isRunning()) {
                terminateInstance(bouncingInstance);
            }
        }
    }

    /**
     * Hazelcast 3.9 knows the {@link FirewallingConnectionManager}.
     */
    @Test
    public void testMemberWithConfig_withFirewallingConnectionManager() {
        testMemberWithConfig("3.9", true);
    }

    /**
     * Hazelcast 3.7 doesn't know the {@link FirewallingConnectionManager}.
     */
    @Test
    public void testMemberWithConfig_withoutFirewallingConnectionManager() {
        testMemberWithConfig("3.7", false);
    }

    private void testMemberWithConfig(String version, boolean supportsFirewallingConnectionManager) {
        Config config = new Config()
                .setInstanceName("test-name");

        hz = HazelcastStarter.newHazelcastInstance(version, config, false);
        assertEquals(hz.getName(), "test-name");

        Node node = HazelcastStarter.getNode(hz);
        assertNotNull(node);
        assertEquals("Expected the same address from HazelcastInstance and Node",
                hz.getCluster().getLocalMember().getAddress(), node.getThisAddress());
        if (supportsFirewallingConnectionManager) {
            assertInstanceOf(FirewallingConnectionManager.class, node.getConnectionManager());
        } else {
            assertInstanceOf(TcpIpConnectionManager.class, node.getConnectionManager());
        }
    }

    @Test
    public void testGetOrCreateWorkingDir() {
        String versionSpec = "3.10-EE-test";
        File dir = HazelcastStarter.getOrCreateVersionDirectory(versionSpec);
        assertTrue("Temporary directory should have been created", dir.exists());
        String path = dir.getAbsolutePath();
        // ensure no exception is thrown when attempting to recreate an existing version directory
        dir = HazelcastStarter.getOrCreateVersionDirectory(versionSpec);
        assertEquals(path, dir.getAbsolutePath());
    }

    @Test
    public void testHazelcastInstanceCompatibility_withStarterInstance() {
        Config config = new Config();
        hz = HazelcastStarter.newHazelcastInstance("3.10.3", config, false);
        testHazelcastInstanceCompatibility(hz, null);
    }

    @Test
    public void testHazelcastInstanceCompatibility_withRealInstance() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        try {
            testHazelcastInstanceCompatibility(instance, null);
        } finally {
            terminateInstance(instance);
        }
    }

    @Test
    public void testHazelcastInstanceCompatibility_withFactoryInstance() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        try {
            testHazelcastInstanceCompatibility(instance, factory);
        } finally {
            factory.terminateAll();
        }
    }

    private static void testHazelcastInstanceCompatibility(HazelcastInstance instance, TestHazelcastInstanceFactory factory) {
        Node node = TestUtil.getNode(instance);
        HazelcastInstanceImpl instanceImpl = TestUtil.getHazelcastInstanceImpl(instance);
        if (factory != null) {
            assertEquals("Expected one active HazelcastInstance in the factory", 1, factory.getAllHazelcastInstances().size());
        }

        assertNode(node, true);
        assertHazelcastInstanceImpl(instanceImpl);
        assertSafePartitionServiceState(instance);

        instance.shutdown();
        assertNode(node, false);
        if (factory != null) {
            assertEquals("Expected no active HazelcastInstances in the factory", 0, factory.getAllHazelcastInstances().size());
        }

        assertGetNodeFromShutdownInstance(instance);
        assertGetHazelcastInstanceImplFromShutdownInstance(instance);
        assertSafePartitionServiceState(instance);
    }

    private static void assertNode(Node node, boolean isRunning) {
        assertNotNull(node);
        assertEquals(isRunning, node.isRunning());
    }

    private static void assertGetNodeFromShutdownInstance(HazelcastInstance hz) {
        try {
            TestUtil.getNode(hz);
            fail("Expected IllegalArgumentException from TestUtil.getNode()");
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
    }

    private static void assertHazelcastInstanceImpl(HazelcastInstanceImpl hazelcastInstanceImpl) {
        assertNotNull(hazelcastInstanceImpl);
    }

    private static void assertGetHazelcastInstanceImplFromShutdownInstance(HazelcastInstance hz) {
        try {
            TestUtil.getHazelcastInstanceImpl(hz);
            fail("Expected IllegalArgumentException from TestUtil.getHazelcastInstanceImpl()");
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
    }

    private static void assertSafePartitionServiceState(HazelcastInstance realInstance) {
        PartitionServiceState partitionServiceState = TestPartitionUtils.getPartitionServiceState(realInstance);
        assertEquals(PartitionServiceState.SAFE, partitionServiceState);
    }
}
