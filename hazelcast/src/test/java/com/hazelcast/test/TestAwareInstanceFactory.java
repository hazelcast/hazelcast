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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;

/**
 * Per test-method factory for Hazelcast members. It sets existing members host:port from given cluster to TCP join
 * configuration of other members. The instances are kept per test method which allows to terminate them in
 * {@link org.junit.After} methods (see {@link #terminateAll()}). The factory methods also sets custom group name which prevents
 * accidental joins (e.g. dangling members).
 * <p>
 * Usage of {@link com.hazelcast.test.annotation.ParallelTest} is allowed with this instance factory.<br/>
 * Example:
 *
 * <pre>
 * &commat;RunWith(HazelcastParallelClassRunner.class)
 * &commat;Category({QuickTest.class, ParallelTest.class})
 * public class Test {
 *
 *     private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
 *
 *     &commat;After
 *     public void after() {
 *         factory.terminateAll();
 *     }
 *
 *     &commat;Test
 *     public void test1() {
 *         Config config = new Config();
 *         HazelcastInstance h1 = factory.newHazelcastInstance(config);
 *         HazelcastInstance h2 = factory.newHazelcastInstance(config);
 *         // ...
 *     }
 *
 *     &commat;Test
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

    protected final Map<String, List<HazelcastInstance>> perMethodMembers = new ConcurrentHashMap<String, List<HazelcastInstance>>();

    /**
     * Creates new member instance with TCP join configured. The value
     * {@link com.hazelcast.test.AbstractHazelcastClassRunner#getTestMethodName()} is used as a cluster group name.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        if (config == null) {
            config = new Config();
        }
        config.getGroupConfig().setName(getTestMethodName());
        List<HazelcastInstance> members = getOrInitInstances(perMethodMembers);
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(PORT.getAndIncrement());
        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig().setEnabled(true);
        for (HazelcastInstance member : members) {
            tcpIpConfig.addMember("127.0.0.1:" + getPort(member));
        }
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        members.add(hz);
        int nextPort = getPort(hz) + 1;
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
                hz.shutdown();
            }
        }
    }

    protected List<HazelcastInstance> getOrInitInstances(Map<String, List<HazelcastInstance>> map) {
        String methodName = getTestMethodName();
        List<HazelcastInstance> list = map.get(methodName);
        if (list == null) {
            list = new ArrayList<HazelcastInstance>();
            map.put(methodName, list);
        }
        return list;
    }

    protected static int getPort(HazelcastInstance hz) {
        return getAddress(hz).getPort();
    }
}
