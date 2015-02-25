/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.util.Clock;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.JumpingSystemClock.JUMP_AFTER_SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * @author mdogan 18/12/14
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SystemClockChangeTest extends HazelcastTestSupport {

    private Object isolatedNode;

    @Before
    @After
    public void killAllHazelcastInstances() throws Exception {
        HazelcastInstanceFactory.terminateAll();
        shutdownIsolatedNode();
        resetClock();
    }

    private void shutdownIsolatedNode() throws Exception {
        if (isolatedNode != null) {
            Class<?> instanceClass = isolatedNode.getClass();
            Method shutdown = instanceClass.getMethod("shutdown");
            shutdown.invoke(isolatedNode);
        }
    }

    @Test
    public void testCluster_whenMasterClockJumpsForward() throws Exception {
        setClock(TimeUnit.MINUTES.toMillis(30));
        HazelcastInstance hz = startNode();

        resetClock();
        isolatedNode = startIsolatedNode();

        assertClusterSizeAlways(2, hz, TimeUnit.SECONDS.toMillis(JUMP_AFTER_SECONDS * 2));
        assertClusterTime(hz.getCluster().getClusterTime(), isolatedNode);
    }

    @Test
    public void testCluster_whenMasterClockJumpsBackward() throws Exception {
        setClock(TimeUnit.MINUTES.toMillis(-30));
        HazelcastInstance hz = startNode();

        resetClock();
        isolatedNode = startIsolatedNode();

        assertClusterSizeAlways(2, hz, TimeUnit.SECONDS.toMillis(JUMP_AFTER_SECONDS * 2));
        assertClusterTime(hz.getCluster().getClusterTime(), isolatedNode);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsForward() throws Exception {
        isolatedNode = startIsolatedNode();

        setClock(TimeUnit.MINUTES.toMillis(30));
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz, TimeUnit.SECONDS.toMillis(JUMP_AFTER_SECONDS * 2));
        assertClusterTime(System.currentTimeMillis(), hz);
    }

    @Test
    public void testCluster_whenSlaveClockJumpsBackward() throws Exception {
        isolatedNode = startIsolatedNode();

        setClock(TimeUnit.MINUTES.toMillis(-30));
        HazelcastInstance hz = startNode();

        assertClusterSizeAlways(2, hz, TimeUnit.SECONDS.toMillis(JUMP_AFTER_SECONDS * 2));
        assertClusterTime(System.currentTimeMillis(), hz);
    }

    private void assertClusterTime(long expected, HazelcastInstance hz) {
        assertEquals("Cluster time should be equal to master time!",
                expected, hz.getCluster().getClusterTime(), 1000d);
    }

    private void assertClusterTime(long expected, Object hz) throws Exception {
        Method getCluster = hz.getClass().getMethod("getCluster");
        Object cluster = getCluster.invoke(hz);
        Method getClusterTime = cluster.getClass().getMethod("getClusterTime");
        Number clusterTime = (Number) getClusterTime.invoke(cluster);

        assertEquals("Cluster time should be equal to master time!", expected,
                clusterTime.doubleValue(), 1000d);
    }

    private void assertClusterSizeAlways(int expected, HazelcastInstance hz, long timeoutMillis) {
        Cluster cluster = hz.getCluster();
        while (timeoutMillis > 0) {
            assertEquals("Cluster should be stable when system clock changes!", expected, cluster.getMembers().size());
            sleepSeconds(1);
            timeoutMillis -= 1000;
        }
    }

    private void setClock(long offset) {
        System.setProperty(Clock.HAZELCAST_CLOCK_IMPL, JumpingSystemClock.class.getName());
        System.setProperty(Clock.HAZELCAST_CLOCK_OFFSET, String.valueOf(offset));
    }

    private void resetClock() {
        System.clearProperty(Clock.HAZELCAST_CLOCK_IMPL);
        System.clearProperty(Clock.HAZELCAST_CLOCK_OFFSET);
    }

    private static HazelcastInstance startNode() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "10");
        return Hazelcast.newHazelcastInstance(config);
    }

    private static Object startIsolatedNode() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            FilteringClassLoader cl = new FilteringClassLoader(Collections.<String>emptyList(),
                    "com.hazelcast");
            thread.setContextClassLoader(cl);

            Class<?> configClazz = cl.loadClass("com.hazelcast.config.Config");
            Object config = configClazz.newInstance();
            Method setClassLoader = configClazz.getDeclaredMethod("setClassLoader", ClassLoader.class);
            setClassLoader.invoke(config, cl);
            Method setProperty = configClazz.getDeclaredMethod("setProperty", String.class, String.class);
            setProperty.invoke(config, GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "10");

            Class<?> hazelcastClazz = cl.loadClass("com.hazelcast.core.Hazelcast");
            Method newHazelcastInstance = hazelcastClazz.getDeclaredMethod("newHazelcastInstance", configClazz);
            return newHazelcastInstance.invoke(hazelcastClazz, config);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }
}

