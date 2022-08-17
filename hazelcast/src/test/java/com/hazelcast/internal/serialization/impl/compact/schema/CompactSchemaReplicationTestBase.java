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
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.instance.impl.TestUtil.getNode;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.getSchemasFor;
import static org.mockito.Mockito.spy;

public class CompactSchemaReplicationTestBase extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    protected static final Schema SCHEMA = getSchemasFor(NodeDTO.class).iterator().next();

    protected HazelcastInstance instance1;
    protected HazelcastInstance instance2;
    protected HazelcastInstance instance3;
    protected HazelcastInstance instance4;

    protected Collection<HazelcastInstance> instances;

    private final CustomTestInstanceFactory factory = new CustomTestInstanceFactory();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        instance3 = factory.newHazelcastInstance(getConfig());
        instance4 = factory.newHazelcastInstance(getConfig());

        instances = Arrays.asList(instance1, instance2, instance3, instance4);
    }

    @Override
    public Config getConfig() {
        return smallInstanceConfig();
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
        @Override
        public HazelcastInstance newHazelcastInstance(Config config) {
            String instanceName = config != null ? config.getInstanceName() : null;
            NodeContext nodeContext;
            if (TestEnvironment.isMockNetwork()) {
                config = initOrCreateConfig(config);
                nodeContext = this.registry.createNodeContext(this.nextAddress(config.getNetworkConfig().getPort()));
            } else {
                nodeContext = new DefaultNodeContext();
            }
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName,
                    new MemberSchemaServiceMockingNodeContext(nodeContext));
        }
    }

    private static class MemberSchemaServiceMockingNodeContext implements NodeContext {

        private final NodeContext delegate;

        private MemberSchemaServiceMockingNodeContext(NodeContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new MemberSchemaServiceMockingNodeExtension(node);
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

        MemberSchemaServiceMockingNodeExtension(Node node) {
            super(node);
            schemaService = spy(new MemberSchemaService());
        }

        @Override
        public MemberSchemaService createSchemaService() {
            return schemaService;
        }
    }
}
