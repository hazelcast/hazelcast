/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import example.serialization.NodeDTO;
import org.junit.After;

import java.util.function.Function;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.getSchemasFor;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockingDetails;

public class CompactSchemaReplicationTestBase extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    protected static final Schema SCHEMA = getSchemasFor(NodeDTO.class).iterator().next();

    protected HazelcastInstance[] instances;

    private final CustomTestInstanceFactory factory = new CustomTestInstanceFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    public void setupInstances(Function<Integer, MemberSchemaService> spyServiceCreator) {
        int instanceCount = 4;
        instances = new HazelcastInstance[instanceCount];
        for (int i = 0; i < 4; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig(), spyServiceCreator.apply(i));
        }
    }

    @Override
    public Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    protected void fillMapUsing(HazelcastInstance instance) {
        IMap<Integer, NodeDTO> map = instance.getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, new NodeDTO(i));
        }
    }

    protected MemberSchemaService getSchemaService(HazelcastInstance instance) {
        return getNode(instance).getSchemaService();
    }

    private static class CustomTestInstanceFactory extends TestHazelcastInstanceFactory {
        public HazelcastInstance newHazelcastInstance(Config config, MemberSchemaService schemaService) {
            assertTrue(mockingDetails(schemaService).isSpy());

            String instanceName = config != null ? config.getInstanceName() : null;
            NodeContext nodeContext;
            if (TestEnvironment.isMockNetwork()) {
                config = initOrCreateConfig(config);
                nodeContext = this.registry.createNodeContext(this.nextAddress(config.getNetworkConfig().getPort()));
            } else {
                nodeContext = new DefaultNodeContext();
            }
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName,
                    new MemberSchemaServiceMockingNodeContext(nodeContext, schemaService));
        }
    }

    private static class MemberSchemaServiceMockingNodeContext implements NodeContext {

        private final NodeContext delegate;
        private final MemberSchemaService schemaService;

        private MemberSchemaServiceMockingNodeContext(NodeContext delegate, MemberSchemaService schemaService) {
            this.delegate = delegate;
            this.schemaService = schemaService;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new MemberSchemaServiceMockingNodeExtension(node, schemaService);
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return delegate.createAddressPicker(node);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return delegate.createJoiner(node);
        }

        @Override
        public Server createServer(Node node, ServerSocketRegistry serverSocketRegistry, LocalAddressRegistry addressRegistry) {
            return delegate.createServer(node, serverSocketRegistry, addressRegistry);
        }
    }

    private static class MemberSchemaServiceMockingNodeExtension extends DefaultNodeExtension {
        private final MemberSchemaService schemaService;

        MemberSchemaServiceMockingNodeExtension(Node node, MemberSchemaService schemaService) {
            super(node);
            this.schemaService = schemaService;
        }

        @Override
        public MemberSchemaService createSchemaService() {
            return schemaService;
        }
    }
}
