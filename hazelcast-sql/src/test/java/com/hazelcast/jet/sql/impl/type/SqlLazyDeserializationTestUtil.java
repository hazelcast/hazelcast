/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;

import com.hazelcast.internal.serialization.InternalSerializationService;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class SqlLazyDeserializationTestUtil {
    public static class SqlLazyDeserializationTestInstanceFactory extends TestHazelcastFactory {
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
                    new SerializationServiceMockingNodeContext(nodeContext));
        }
    }

    public static class SerializationServiceMockingNodeContext implements NodeContext {

        private final NodeContext delegate;

        private SerializationServiceMockingNodeContext(NodeContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new SerializationServiceMockingNodeExtension(node);
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

    public static class SerializationServiceMockingNodeExtension extends DefaultNodeExtension {

        SerializationServiceMockingNodeExtension(Node node) {
            super(node);
        }

        @Override
        public InternalSerializationService createSerializationService() {
            return spy(super.createSerializationService());
        }
    }

    public static class ResultCaptor implements Answer<InternalGenericRecord> {
        private CopyOnWriteArrayList<InternalGenericRecord> results = new CopyOnWriteArrayList<>();
        public CopyOnWriteArrayList<InternalGenericRecord> getResults() {
            return results;
        }

        @Override
        public InternalGenericRecord answer(InvocationOnMock invocationOnMock) throws Throwable {
            InternalGenericRecord spiedResult = spy((InternalGenericRecord) invocationOnMock.callRealMethod());
            doAnswer(this).when(spiedResult).getInternalGenericRecord(any(String.class));
            results.add(spiedResult);
            return spiedResult;
        }
    }
}
