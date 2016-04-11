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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.util.Clock;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 18/12/14
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SystemClockChangeTest extends HazelcastTestSupport {

    private static final int JUMP_AFTER_SECONDS = 15;

    private Object isolatedNode;

    @BeforeClass
    public static void setSystemProps() {
        System.setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        System.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
    }

    @AfterClass
    public static void clearSystemProps() {
        System.clearProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName());
        System.clearProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName());
    }

    @Before
    @After
    public void killAllHazelcastInstances() throws Exception {
        HazelcastInstanceManager.terminateAll();
        shutdownIsolatedNode();
        resetClock();
    }

    private void shutdownIsolatedNode() throws Exception {
        if (isolatedNode != null) {
            Class<?> instanceClass = isolatedNode.getClass();
            Method shutdown = instanceClass.getMethod("shutdown");
            shutdown.invoke(isolatedNode);
            isolatedNode = null;
        }
    }

    @Test
    public void testCluster_whenMasterClockIsBehind() throws Exception {
        setClockOffset(TimeUnit.MINUTES.toMillis(-60));
        isolatedNode = startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSize(2, hz);
        assertClusterTime(getClusterTime(isolatedNode), hz);
    }

    @Test
    public void testCluster_whenMasterClockIsAhead() throws Exception {
        setClockOffset(TimeUnit.MINUTES.toMillis(60));
        isolatedNode = startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSize(2, hz);
        assertClusterTime(getClusterTime(isolatedNode), hz);
    }

    @Test
    public void testCluster_whenMasterChanges() throws Exception {
        setClockOffset(TimeUnit.MINUTES.toMillis(60));
        isolatedNode = startIsolatedNode();

        resetClock();
        HazelcastInstance hz1 = startNode();
        HazelcastInstance hz2 = startNode();

        assertClusterSizeEventually(3, hz1);
        assertClusterSizeEventually(3, hz2);

        shutdownIsolatedNode();

        assertClusterSizeEventually(2, hz1);
        assertClusterSizeEventually(2, hz2);

        assertClusterTime(hz1.getCluster().getClusterTime(), hz2);
    }

    @Test
    public void testCluster_whenMasterClockJumpsForward() throws Exception {
        setJumpingClock(TimeUnit.MINUTES.toMillis(30));
        isolatedNode = startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(hz.getCluster().getClusterTime(), isolatedNode);
    }

    @Test
    public void testCluster_whenMasterClockJumpsBackward() throws Exception {
        setJumpingClock(TimeUnit.MINUTES.toMillis(-30));
        isolatedNode = startIsolatedNode();

        resetClock();
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(hz.getCluster().getClusterTime(), isolatedNode);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsForward() throws Exception {
        HazelcastInstance hz = startNode();

        setJumpingClock(TimeUnit.MINUTES.toMillis(30));
        isolatedNode = startIsolatedNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(System.currentTimeMillis(), hz);
        assertClusterTime(System.currentTimeMillis(), isolatedNode);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsBackward() throws Exception {
        HazelcastInstance hz = startNode();

        setJumpingClock(TimeUnit.MINUTES.toMillis(-30));
        isolatedNode = startIsolatedNode();

        assertClusterSizeAlways(2, hz);
        assertClusterTime(System.currentTimeMillis(), hz);
        assertClusterTime(System.currentTimeMillis(), isolatedNode);
    }

    private void assertClusterTime(long expected, HazelcastInstance hz) {
        assertEquals("Cluster time should be (approx.) equal to master time!",
                expected, hz.getCluster().getClusterTime(), 1000d);
    }

    private void assertClusterTime(long expected, Object hz) throws Exception {
        assertEquals("Cluster time should be (approx.) equal to master time!", expected,
                getClusterTime(hz), 1000d);
    }

    private long getClusterTime(Object hz) throws Exception {
        Method getCluster = hz.getClass().getMethod("getCluster");
        Object cluster = getCluster.invoke(hz);
        Method getClusterTime = cluster.getClass().getMethod("getClusterTime");
        return ((Number) getClusterTime.invoke(cluster)).longValue();
    }

    private void assertClusterSizeAlways(final int expected, HazelcastInstance hz) {
        final Cluster cluster = hz.getCluster();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Cluster should be stable when system clock changes!", expected, cluster.getMembers().size());
            }
        }, JUMP_AFTER_SECONDS * 2);
    }

    private void setClockOffset(long offset) {
        System.setProperty(Clock.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
    }

    private void setJumpingClock(long offset) {
        System.setProperty(Clock.HAZELCAST_CLOCK_IMPL, JumpingSystemClock.class.getName());
        System.setProperty(Clock.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
        System.setProperty(JumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY, String.valueOf(JUMP_AFTER_SECONDS));
    }

    private void resetClock() {
        System.clearProperty(Clock.HAZELCAST_CLOCK_IMPL);
        System.clearProperty(Clock.HAZELCAST_CLOCK_OFFSET);
        System.clearProperty(JumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY);
    }

    private static HazelcastInstance startNode() throws Exception {
        Config config = new Config();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static Object startIsolatedNode() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            FilteringClassLoader cl = new FilteringClassLoader(Collections.<String>emptyList(), "com.hazelcast");
            thread.setContextClassLoader(cl);

            Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
            setClassLoader.invoke(config, cl);

            Class<?> hazelcastClazz = cl.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);
            return newHazelcastInstance.invoke(hazelcastClazz, config);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }
}
