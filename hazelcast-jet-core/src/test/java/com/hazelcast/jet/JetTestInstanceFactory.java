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

package com.hazelcast.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.util.Arrays;

import static com.hazelcast.jet.Jet.getJetClientInstance;
import static com.hazelcast.jet.impl.JetNodeContext.JET_EXTENSION_PRIORITY_LIST;
import static com.hazelcast.jet.impl.config.XmlJetConfigBuilder.getClientConfig;

public class JetTestInstanceFactory {

    private final TestHazelcastFactory factory = new TestHazelcastFactoryForJet();

    private static class TestHazelcastFactoryForJet extends TestHazelcastFactory {
        @Override
        protected TestNodeRegistry createRegistry() {
            return new TestNodeRegistry(getKnownAddresses(), JET_EXTENSION_PRIORITY_LIST);
        }
    }

    public JetInstance newMember() {
        return newMember(JetConfig.loadDefault());
    }

    public JetInstance newMember(JetConfig config) {
        return Jet.newJetInstanceImpl(config, factory::newHazelcastInstance);
    }

    public JetInstance newMember(JetConfig config, Address[] blockedAddresses) {
        return Jet.newJetInstanceImpl(config, hzCfg -> factory.newHazelcastInstance(hzCfg, blockedAddresses));
    }

    public JetInstance[] newMembers(JetConfig config, int nodeCount) {
        JetInstance[] jetInstances = new JetInstance[nodeCount];
        Arrays.setAll(jetInstances, i -> newMember(config));
        return jetInstances;
    }


    public JetClientInstanceImpl newClient() {
        return newClient(getClientConfig());
    }

    public JetClientInstanceImpl newClient(ClientConfig config) {
        HazelcastInstance client = factory.newHazelcastClient(config);
        return getJetClientInstance(client);
    }

    public void shutdownAll() {
        factory.shutdownAll();
    }

    public void terminateAll() {
        factory.terminateAll();
    }

    public void terminate(JetInstance instance) {
        factory.terminate(instance.getHazelcastInstance());
    }

    public Address nextAddress() {
        return factory.nextAddress();
    }
}
