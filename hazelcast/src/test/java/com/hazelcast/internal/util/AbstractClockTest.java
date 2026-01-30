/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Cluster;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastStarter;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;

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
    private HazelcastAPIDelegatingClassloader isolatedClassLoader;
    protected HazelcastInstance isolatedNode;

    protected HazelcastInstance startNode() {
        Config config = getConfig();
        return Hazelcast.newHazelcastInstance(config);
    }

    protected void startIsolatedNode() {
        startIsolatedNode(new Config());
    }

    protected void startIsolatedNode(Config config) {
        if (isolatedNode != null) {
            throw new IllegalStateException("There is already an isolated node running!");
        }

        try {
            URL classesUrl = new File("target/classes/").toURI().toURL();
            URL tpcClassesUrl = new File("../hazelcast-tpc-engine/target/classes/").toURI().toURL();
            ClassLoader classLoader = getClass().getClassLoader();
            isolatedClassLoader = new HazelcastAPIDelegatingClassloader(
                new URL[]{classesUrl, tpcClassesUrl},
                classLoader
            );
            isolatedNode = HazelcastStarter.newHazelcastInstance(config, isolatedClassLoader);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Could not create Hazelcast member with custom classloader", e);
        }
    }

    protected void shutdownIsolatedNode() {
        if (isolatedNode == null) {
            return;
        }
        isolatedNode.shutdown();
    }

    protected static void setClockOffset(long offset) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
    }

    protected static void setJumpingClock(long offset) {
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_IMPL, TestJumpingSystemClock.class.getName());
        System.setProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
        System.setProperty(TestJumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY, String.valueOf(JUMP_AFTER_SECONDS));
    }

    protected static void resetClock() {
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_IMPL);
        System.clearProperty(ClockProperties.HAZELCAST_CLOCK_OFFSET);
        System.clearProperty(TestJumpingSystemClock.JUMP_AFTER_SECONDS_PROPERTY);
    }

    protected static long getClusterTime(HazelcastInstance isolatedNode) {
        return isolatedNode.getCluster().getClusterTime();
    }

    @SuppressWarnings("unchecked")
    protected <T> T invokeIsolatedInstanceMethod(
        String className,
        String methodName,
        Class<?>[] parameterTypes,
        Object... args
    ) {
        try {
            Class<?> clazz = Class.forName(className, true, isolatedClassLoader);
            Object instance = clazz.getDeclaredConstructor().newInstance();

            Method method = clazz.getMethod(methodName, parameterTypes);
            return (T) method.invoke(instance, args);

        } catch (InvocationTargetException e) {
            throw rethrowTargetException(e.getTargetException(), className, methodName);

        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                "Could not invoke method " + methodName + " on object " + className, e
            );
        }
    }

    protected <T> T invokeIsolatedInstanceMethod(
        String className,
        String methodName
    ) {
        return invokeIsolatedInstanceMethod(
            className,
            methodName,
            new Class<?>[0]
        );
    }

    private RuntimeException rethrowTargetException(
        Throwable target,
        String className,
        String methodName
    ) {
        if (target instanceof RuntimeException rex) {
            throw rex;
        }
        if (target instanceof Error err) {
            throw err;
        }

        return new RuntimeException(
            "Could not invoke method " + methodName + " on object " + className,
            target
        );
    }

    protected static void assertClusterTime(HazelcastInstance expectedHz, HazelcastInstance hz) {
        assertClusterTime(expectedHz.getCluster().getClusterTime(), hz);
    }

    protected static void assertClusterTime(long expected, HazelcastInstance hz) {
        assertClusterTime(expected, hz.getCluster().getClusterTime());
    }

    protected static void assertClusterSizeAlways(final int expected, HazelcastInstance hz) {
        final Cluster cluster = hz.getCluster();
        assertTrueAllTheTime(() -> assertEquals("Cluster should be stable when system clock changes!", expected, cluster.getMembers().size()), JUMP_AFTER_SECONDS * 2);
    }

    private static void assertClusterTime(long expected, long actual) {
        assertEquals("Cluster time should be (approx.) equal to master time!", expected, actual, 1000d);
    }
}
