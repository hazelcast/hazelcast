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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.test.TestAwareInstanceFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.config.Config.DEFAULT_CLUSTER_NAME;
import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;

/**
 * Per test-method factory for Hazelcast clients (and also members as it inherits from {@link TestAwareInstanceFactory}).
 * It configures new clients in the same way as it's done for members in
 * {@link TestAwareInstanceFactory#newHazelcastInstance(com.hazelcast.config.Config)}.
 * <p>
 * <b>Tests using this factory should not be annotated with {@code ParallelJVMTest} category to avoid runs in multiple JVMs.</b>
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class TestAwareClientFactory extends TestAwareInstanceFactory {

    protected final Map<String, List<HazelcastInstance>> perMethodClients = new ConcurrentHashMap<String, List<HazelcastInstance>>();

    /**
     * Creates new client instance which uses in its network configuration the first member created by this factory. The value
     * {@link com.hazelcast.test.AbstractHazelcastClassRunner#getTestMethodName()} is used as the cluster name if it's not
     * changed already.
     */
    public HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (config == null) {
            config = new ClientConfig();
        }
        if (DEFAULT_CLUSTER_NAME.equals(config.getClusterName())) {
            config.setClusterName(getTestMethodName());
        }
        List<HazelcastInstance> members = getOrInitInstances(perMethodMembers);
        if (members.isEmpty()) {
            throw new IllegalStateException("Members have to be created first");
        }
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        for (HazelcastInstance member : members) {
            networkConfig.addAddress("127.0.0.1:" + getPort(member, EndpointQualifier.CLIENT));
        }
        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);
        getOrInitInstances(perMethodClients).add(hz);
        return hz;
    }

    /**
     * Terminates all client and member instances created by this factory for current test method name.
     */
    @Override
    public void terminateAll() {
        shutdownInstances(perMethodClients.remove(getTestMethodName()));
        super.terminateAll();
    }
}
