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

package com.hazelcast.internal.util;

import com.hazelcast.config.Config;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Base class for tests which have to change the {@link Clock} implementation, which works properly only in an isolated node.
 * <p>
 * Use {@link #startIsolatedNode()} to create an isolated node, which will pick up the actual {@link ClockProperties}.
 * Use {@link #shutdownIsolatedNode()} and {@link #resetClock()} to stop the node and cleanup the properties.
 * <p>
 * Implementations of this class have to run in full isolation, so {@link com.hazelcast.test.HazelcastSerialClassRunner} and
 * no usage of {@link ParallelJVMTest}.
 */
public abstract class AbstractClockTest extends HazelcastTestSupport {

    private static final int JUMP_AFTER_SECONDS = 15;

    protected Object isolatedNode;

    protected HazelcastInstance startNode() {
        Config config = getConfig();
        return Hazelcast.newHazelcastInstance(config);
    }

    protected void startIsolatedNode() {
        if (isolatedNode != null) {
            throw new IllegalStateException("There is already an isolated node running!");
        }
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
            isolatedNode = newHazelcastInstance.invoke(hazelcastClazz, config);
        } catch (Exception e) {
            throw new RuntimeException("Could not start isolated Hazelcast instance", e);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    protected void shutdownIsolatedNode() {
        if (isolatedNode == null) {
            return;
        }
        try {
            Class<?> instanceClass = isolatedNode.getClass();
            Method method = instanceClass.getMethod("shutdown");
            method.invoke(isolatedNode);
            isolatedNode = null;
        } catch (Exception e) {
            throw new RuntimeException("Could not start shutdown Hazelcast instance", e);
        }
    }

    protected static void setClockOffset(long offset) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
    }

    protected static void setJumpingClock(long offset) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_IMPL, JumpingSystemClock.class.getName());
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
        System.setProperty(JumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY, String.valueOf(JUMP_AFTER_SECONDS));
    }

    protected static void resetClock() {
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_IMPL);
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
        System.clearProperty(JumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY);
    }

    protected static long getClusterTime(Object isolatedNode) {
        try {
            Method getCluster = isolatedNode.getClass().getMethod("getCluster");
            Object cluster = getCluster.invoke(isolatedNode);
            Method getClusterTime = cluster.getClass().getMethod("getClusterTime");
            return ((Number) getClusterTime.invoke(cluster)).longValue();
        } catch (Exception e) {
            throw new RuntimeException("Could not get cluster time from Hazelcast instance", e);
        }
    }

    protected static void assertClusterTime(HazelcastInstance expectedHz, Object isolatedNode) {
        assertClusterTime(expectedHz.getCluster().getClusterTime(), isolatedNode);
    }

    protected static void assertClusterTime(long expected, Object isolatedNode) {
        assertClusterTime(expected, getClusterTime(isolatedNode));
    }

    protected static void assertClusterTime(Object expectedIsolatedNode, HazelcastInstance hz) {
        assertClusterTime(getClusterTime(expectedIsolatedNode), hz);
    }

    protected static void assertClusterTime(HazelcastInstance expectedHz, HazelcastInstance hz) {
        assertClusterTime(expectedHz.getCluster().getClusterTime(), hz);
    }

    protected static void assertClusterTime(long expected, HazelcastInstance hz) {
        assertClusterTime(expected, hz.getCluster().getClusterTime());
    }

    protected static void assertClusterSizeAlways(final int expected, HazelcastInstance hz) {
        final Cluster cluster = hz.getCluster();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Cluster should be stable when system clock changes!", expected, cluster.getMembers().size());
            }
        }, JUMP_AFTER_SECONDS * 2);
    }

    private static void assertClusterTime(long expected, long actual) {
        assertEquals("Cluster time should be (approx.) equal to master time!", expected, actual, 1000d);
    }
}
