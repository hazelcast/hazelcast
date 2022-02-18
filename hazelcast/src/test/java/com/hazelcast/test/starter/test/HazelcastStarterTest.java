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

package com.hazelcast.test.starter.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.partition.TestPartitionUtils;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.internal.server.FirewallingServer;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
@Ignore("To be enabled in 4.1 with 4.x instances - see https://github.com/hazelcast/hazelcast/issues/15263")
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
    // reason for ignore: The ConnectionManager interface got replaced
    // by EndpointManager and NetworkingService in 3.12. This test is
    // supposed to verify behavior for split-brain compatibility tests
    // that needs a considerable effort to adapt
    // FirewallingConnectionManager to 3.12's NetworkingService. This test
    // therefore got ignored until the decisions is made whether or not
    // to support split-brain compatibility tests with the recent changes.
    @Ignore
    @Test
    public void testMemberWithConfig_withFirewallingConnectionManager() {
        testMemberWithConfig("3.9", true);
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
            assertInstanceOf(FirewallingServer.class, node.getServer());
        } else {
            assertInstanceOf(TcpServer.class, node.getServer());
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
