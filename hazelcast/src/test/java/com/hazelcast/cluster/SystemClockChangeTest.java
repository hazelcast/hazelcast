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

package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.internal.util.AbstractClockTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SystemClockChangeTest extends AbstractClockTest {

    @BeforeClass
    public static void setSystemProps() {
        System.setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        System.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
    }

    @AfterClass
    public static void clearSystemProps() {
        System.clearProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName());
        System.clearProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName());
    }

    @After
    public void tearDown() {
        HazelcastInstanceFactory.terminateAll();
        shutdownIsolatedNode();
        resetClock();
    }

    @Test
    public void testCluster_whenMasterClockIsBehind() {
        setClockOffset(TimeUnit.MINUTES.toMillis(-60));
        startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSize(2, hz);
        assertClusterTime(isolatedNode, hz);
    }

    @Test
    public void testCluster_whenMasterClockIsAhead() {
        setClockOffset(TimeUnit.MINUTES.toMillis(60));
        startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSize(2, hz);
        assertClusterTime(isolatedNode, hz);
    }

    @Test
    public void testCluster_whenMasterChanges() {
        setClockOffset(TimeUnit.MINUTES.toMillis(60));
        startIsolatedNode();

        resetClock();
        HazelcastInstance hz1 = startNode();
        HazelcastInstance hz2 = startNode();

        assertClusterSizeEventually(3, hz1, hz2);

        shutdownIsolatedNode();

        assertClusterSizeEventually(2, hz1, hz2);

        assertClusterTime(hz1, hz2);
    }

    @Test
    public void testCluster_whenMasterClockJumpsForward() {
        setJumpingClock(TimeUnit.MINUTES.toMillis(30));
        startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(hz, isolatedNode);
    }

    @Test
    public void testCluster_whenMasterClockJumpsBackward() {
        setJumpingClock(TimeUnit.MINUTES.toMillis(-30));
        startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(hz, isolatedNode);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsForward() {
        HazelcastInstance hz = startNode();

        setJumpingClock(TimeUnit.MINUTES.toMillis(30));
        startIsolatedNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(System.currentTimeMillis(), hz);
        assertClusterTime(System.currentTimeMillis(), isolatedNode);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsBackward() {
        HazelcastInstance hz = startNode();

        setJumpingClock(TimeUnit.MINUTES.toMillis(-30));
        startIsolatedNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(System.currentTimeMillis(), hz);
        assertClusterTime(System.currentTimeMillis(), isolatedNode);
    }
}
