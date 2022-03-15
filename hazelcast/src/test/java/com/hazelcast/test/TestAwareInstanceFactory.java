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

package com.hazelcast.test;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.test.annotation.ParallelJVMTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.Config.DEFAULT_CLUSTER_NAME;
import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.Accessors.getAddress;

/**
 * Per test-method factory for Hazelcast members. It sets existing members host:port from given cluster to TCP join
 * configuration of other members. The instances are kept per test method which allows to terminate them in
 * {@link org.junit.After} methods (see {@link #terminateAll()}). The factory methods also sets custom cluster name which prevents
 * accidental joins (e.g. dangling members).
 * <p>
 * <b>Tests using this factory should not be annotated with {@link ParallelJVMTest} category to avoid runs in multiple JVMs.</b>
 * <p>
 * Example:
 *
 * <pre>
 * &#64;RunWith(HazelcastParallelClassRunner.class)
 * &#64;Category(QuickTest.class)
 * public class Test {
 *
 *     private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
 *
 *     &#64;After
 *     public void after() {
 *         factory.terminateAll();
 *     }
 *
 *     &#64;Test
 *     public void test1() {
 *         Config config = new Config();
 *         HazelcastInstance h1 = factory.newHazelcastInstance(config);
 *         HazelcastInstance h2 = factory.newHazelcastInstance(config);
 *         // ...
 *     }
 *
 *     &#64;Test
 *     public void test2() {
 *         Config config = new Config();
 *         HazelcastInstance h1 = factory.newHazelcastInstance(config);
 *         HazelcastInstance h2 = factory.newHazelcastInstance(config);
 *         // ...
 *     }
 * }
 * </pre>
 */
public class TestAwareInstanceFactory {

    private static final AtomicInteger PORT = new AtomicInteger(5000);

    protected final Map<String, List<HazelcastInstance>> perMethodMembers = new ConcurrentHashMap<>();

    /**
     * Calls {@link #newHazelcastInstance(Config, NodeContext)} using the
     * {@link DefaultNodeContext}.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        return newHazelcastInstance(config, new DefaultNodeContext());
    }

    /**
     * Creates new member instance with TCP join configured. Uses
     * {@link com.hazelcast.test.AbstractHazelcastClassRunner#getTestMethodName()}
     * as the cluster name if it's not changed already.
     */
    public HazelcastInstance newHazelcastInstance(Config config, NodeContext nodeCtx) {
        if (config == null) {
            config = new Config();
        }
        if (DEFAULT_CLUSTER_NAME.equals(config.getClusterName())) {
            config.setClusterName(getTestMethodName());
        }
        List<HazelcastInstance> members = getOrInitInstances(perMethodMembers);

        // Prepare Unified Networking (legacy)
        NetworkConfig unifiedNetworkingConfig = config.getNetworkConfig();
        unifiedNetworkingConfig.setPort(PORT.getAndIncrement());
        JoinConfig unifiedJoinConfig = unifiedNetworkingConfig.getJoin();
        unifiedJoinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig unifiedTcpIpConfig = unifiedJoinConfig.getTcpIpConfig().setEnabled(true);
        for (HazelcastInstance member : members) {
            unifiedTcpIpConfig.addMember("127.0.0.1:" + getPort(member, MEMBER));
        }

        // Prepare Advanced Networking - Will be disabled by default but properly configured if needed
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        ServerSocketEndpointConfig memberEndpointConfig
                = (ServerSocketEndpointConfig) advancedNetworkConfig.getEndpointConfigs().get(MEMBER);
        memberEndpointConfig.setPort(PORT.getAndIncrement());
        ServerSocketEndpointConfig clientEndpointConfig = (ServerSocketEndpointConfig) advancedNetworkConfig
                .getEndpointConfigs().get(CLIENT);
        if (clientEndpointConfig != null) {
            clientEndpointConfig.setPort(PORT.getAndIncrement());
        }
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig advancedTcpIpConfig = advancedJoinConfig.getTcpIpConfig().setEnabled(true);
        for (HazelcastInstance member : members) {
            advancedTcpIpConfig.addMember("127.0.0.1:" + getPort(member, MEMBER));
        }

        HazelcastInstance hz = HazelcastInstanceFactory.newHazelcastInstance(
                config, config.getInstanceName(), nodeCtx);
        members.add(hz);
        int nextPort = Math.max(getPort(hz, MEMBER), getPort(hz, CLIENT)) + 1;
        int current;
        while (nextPort > (current = PORT.get())) {
            PORT.compareAndSet(current, nextPort);
        }
        return hz;
    }

    /**
     * Terminates all member instances created by this factory for current test method name.
     */
    public void terminateAll() {
        shutdownInstances(perMethodMembers.remove(getTestMethodName()));
    }

    protected void shutdownInstances(List<HazelcastInstance> listToRemove) {
        if (listToRemove != null) {
            for (HazelcastInstance hz : listToRemove) {
                ManagementService.shutdown(hz.getName());
                hz.getLifecycleService().terminate();
            }
        }
    }

    protected List<HazelcastInstance> getOrInitInstances(Map<String, List<HazelcastInstance>> map) {
        String methodName = getTestMethodName();
        List<HazelcastInstance> list = map.computeIfAbsent(methodName, k -> new ArrayList<>());
        return list;
    }

    protected static int getPort(HazelcastInstance hz, EndpointQualifier qualifier) {
        return getAddress(hz, qualifier).getPort();
    }
}
