/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Jet.getJetClientInstance;
import static com.hazelcast.jet.impl.JetNodeContext.JET_EXTENSION_PRIORITY_LIST;
import static com.hazelcast.jet.impl.config.XmlJetConfigBuilder.getClientConfig;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static java.util.stream.Collectors.toList;

public class JetTestInstanceFactory {

    private final TestHazelcastFactory factory;

    public JetTestInstanceFactory() {
        factory = new TestHazelcastFactoryForJet();
    }

    public JetTestInstanceFactory(int basePortNumber, String[] addresses) {
        factory = new TestHazelcastFactoryForJet(basePortNumber, addresses);
    }

    public Address nextAddress() {
        return factory.nextAddress();
    }

    public JetInstance newMember() {
        return newMember(JetConfig.loadDefault());
    }

    public JetInstance newMember(JetConfig config) {
        return Jet.newJetInstanceImpl(config, factory::newHazelcastInstance);
    }

    public JetInstance newMember(JetConfig config, Address address) {
        return Jet.newJetInstanceImpl(config, hzCfg -> factory.newHazelcastInstance(address, hzCfg));
    }

    public JetInstance newMember(JetConfig config, Address[] blockedAddresses) {
        return Jet.newJetInstanceImpl(config, hzCfg -> factory.newHazelcastInstance(hzCfg, blockedAddresses));
    }

    public JetInstance[] newMembers(JetConfig config, int nodeCount) {
        JetInstance[] jetInstances = new JetInstance[nodeCount];
        Arrays.setAll(jetInstances, i -> newMember(config));
        return jetInstances;
    }

    /**
     * Creates the given number of Jet instances in parallel. The first one is
     * always master.
     * <p>
     * Spawns a separate thread to start each instance. This is required when
     * starting a Hot Restart-enabled cluster, where the {@code newJetInstance()}
     * call blocks until the whole cluster is re-formed.
     *
     * @param configFn a function that must return a separate config instance for each address
     */
    public JetInstance[] newMembersParallel(int nodeCount, Function<Address, JetConfig> configFn) {
        JetInstance[] jetInstances = IntStream.range(0, nodeCount)
                .mapToObj(i -> factory.nextAddress())
                .map(address -> spawn(() -> newMember(configFn.apply(address), address)))
                // we need to collect here to ensure that all threads are spawned before we call future.get()
                .collect(toList()).stream()
                .map(f -> uncheckCall(f::get))
                .toArray(JetInstance[]::new);
        assertClusterSizeEventually(nodeCount, factory.getAllHazelcastInstances());
        Arrays.sort(jetInstances, Comparator.comparing(inst -> !isMaster(inst)));
        return jetInstances;
    }

    private static boolean isMaster(JetInstance inst) {
        return ((HazelcastInstanceImpl) inst.getHazelcastInstance()).node.isMaster();
    }

    public JetClientInstanceImpl newClient() {
        return newClient(getClientConfig());
    }

    public JetClientInstanceImpl newClient(ClientConfig config) {
        HazelcastInstance client = factory.newHazelcastClient(config);
        return getJetClientInstance(client);
    }

    public JetInstance[] getAllJetInstances() {
        return factory.getAllHazelcastInstances().stream()
                      .map(HazelcastTestSupport::getNodeEngineImpl)
                      .map(node -> node.<JetService>getService(JetService.SERVICE_NAME))
                      .map(JetService::getJetInstance)
                      .toArray(JetInstance[]::new);
    }

    public void terminate(JetInstance instance) {
        factory.terminate(instance.getHazelcastInstance());
    }

    public void shutdownAll() {
        factory.shutdownAll();
    }

    public void terminateAll() {
        factory.terminateAll();
    }

    private static class TestHazelcastFactoryForJet extends TestHazelcastFactory {
        TestHazelcastFactoryForJet() {
        }

        TestHazelcastFactoryForJet(int basePortNumber, String[] addresses) {
            super(basePortNumber, addresses);
        }

        @Override
        protected TestNodeRegistry createRegistry() {
            return new TestNodeRegistry(getKnownAddresses(), JET_EXTENSION_PRIORITY_LIST);
        }
    }
}
