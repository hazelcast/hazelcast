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

package com.hazelcast.test.modulepath;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.ModularJavaUtils;

/**
 * Basic test which checks if correct Hazelcast modules are on the modulepath. It also checks that Hazelcast members and clients
 * are able to start and form a cluster.
 */
@Category(ModulePathTest.class)
public class SmokeModulePathTest {

    @After
    public void after() {
        HazelcastClient.shutdownAll();
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * Verify the Hazelcast JARs are on the modulepath.
     */
    @Test
    public void testModulePath() {
        String modulePath = System.getProperty("jdk.module.path");
        assertNotNull("Module path was expected", modulePath);
        assertTrue("Module path should contain hazelcast JAR",
                modulePath.matches(".*hazelcast-[1-9][\\p{Alnum}\\-_\\.]+\\.jar.*"));
    }

    /**
     * Verify the Hazelcast modules with correct names are used.
     */
    @Test
    public void testModuleNames() {
        Set<String> hazelcastModuleNames = ModuleLayer.boot().modules().stream().map(Module::getName)
                .filter(s -> s.contains("hazelcast")).collect(Collectors.toSet());
        assertThat(hazelcastModuleNames, hasItems("com.hazelcast.core", "com.hazelcast.tests"));
    }

    /**
     * Verify the name of Hazelcast module returned by {@link ModularJavaUtils#getHazelcastModuleName()}.
     */
    @Test
    public void testHazelcastModuleName() {
        assertEquals("com.hazelcast.core", ModularJavaUtils.getHazelcastModuleName());
    }

    /**
     * Verify Hazelcast members are able to start and form cluster. It also verifies the client is able to join and work with
     * the cluster.
     */
    @Test
    public void testCluster() {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getInterfaces().addInterface("127.0.0.1");
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        hz1.getMap("test").put("a", "b");
        assertClusterSize(2, hz1, hz2);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        assertEquals("b", client.getMap("test").get("a"));
    }

    public static void assertClusterSize(int expectedSize, HazelcastInstance... instances) {
        for (int i = 0; i < instances.length; i++) {
            int clusterSize = getClusterSize(instances[i]);
            if (expectedSize != clusterSize) {
                fail(format("Cluster size is not correct. Expected: %d, actual: %d, instance index: %d", expectedSize,
                        clusterSize, i));
            }
        }
    }

    private static int getClusterSize(HazelcastInstance instance) {
        Set<Member> members = instance.getCluster().getMembers();
        return members == null ? 0 : members.size();
    }
}
