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
import com.hazelcast.core.ManagedContext;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.CompactInternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.CompactStreamSerializer;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.internal.serialization.impl.portable.PortableInternalGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableSerializer;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.nio.ObjectDataInput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteOrder;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;

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

    // Only readInternalGenericRecord method is mocked in this serialization service.
    public static final class MockSerializationService implements InternalSerializationService {

        public PortableInternalGenericRecord spyPortableInternalGenericRecord;
        public CompactInternalGenericRecord spyCompactInternalGenericRecord;

        protected final InternalSerializationService delegate;

        public MockSerializationService(InternalSerializationService delegate) {
            this.delegate = delegate;
        }

        @Override
        public InternalGenericRecord readAsInternalGenericRecord(Data data) throws IOException {
            if (data.isPortable()) {
                BufferObjectDataInput in = createObjectDataInput(data);
                PortableInternalGenericRecord internalGenericRecord =
                        (PortableInternalGenericRecord) getPortableSerializer().readAsInternalGenericRecord(in);
                ClassDefinition classDefinition = internalGenericRecord.getClassDefinition();
                if (classDefinition.getFactoryId() == 1 && classDefinition.getClassId() == 1) {
                    if (spyPortableInternalGenericRecord != null) {
                        throw new RuntimeException("This method should be called only once for portable organization type");
                    }
                    spyPortableInternalGenericRecord = spy(internalGenericRecord);
                    return spyPortableInternalGenericRecord;
                }
                return internalGenericRecord;
            }
            if (data.isCompact()) {
                CompactInternalGenericRecord internalGenericRecord = (CompactInternalGenericRecord) getCompactStreamSerializer()
                        .readAsInternalGenericRecord(createObjectDataInput(data));
                String typeName = internalGenericRecord.getSchema().getTypeName();
                if (typeName.equals("OrganizationCompactType")) {
                    if (spyCompactInternalGenericRecord != null) {
                        throw new RuntimeException("This method should be called only once for compact organization type");
                    }
                    spyCompactInternalGenericRecord = spy(internalGenericRecord);
                    return spyCompactInternalGenericRecord;
                }
                return internalGenericRecord;
            }
            throw new IllegalArgumentException("Given type does not support query over data, type id " + data.getType());
        }

        @Override
        public <B extends Data> B toData(Object obj) {
            return delegate.toData(obj);
        }

        @Override
        public <B extends Data> B toDataWithSchema(Object obj) {
            return delegate.toDataWithSchema(obj);
        }

        @Override
        public <B extends Data> B toData(Object obj, PartitioningStrategy strategy) {
            return delegate.toData(obj, strategy);
        }

        @Override
        public void writeObject(ObjectDataOutput out, Object obj) {
            delegate.writeObject(out, obj);
        }

        @Override
        public <T> T readObject(ObjectDataInput in, boolean useBigEndianForReadingTypeId) {
            return delegate.readObject(in, useBigEndianForReadingTypeId);
        }

        @Override
        public <T> T readObject(ObjectDataInput in, Class aClass) {
            return delegate.readObject(in, aClass);
        }

        @Override
        public <T> T toObject(Object data) {
            return delegate.toObject(data);
        }

        @Override
        public <T> T toObject(Object data, Class klazz) {
            return delegate.toObject(data, klazz);
        }

        @Override
        public byte[] toBytes(Object obj) {
            return delegate.toBytes(obj);
        }

        @Override
        public byte[] toBytes(Object obj, int leftPadding, boolean insertPartitionHash) {
            return delegate.toBytes(obj, leftPadding, insertPartitionHash);
        }

        @Override
        public <B extends Data> B toData(Object obj, DataType type) {
            return delegate.toData(obj, type);
        }

        @Override
        public <B extends Data> B toData(Object obj, DataType type, PartitioningStrategy strategy) {
            return delegate.toData(obj, type, strategy);
        }

        @Override
        public <B extends Data> B convertData(Data data, DataType type) {
            return delegate.convertData(data, type);
        }

        @Override
        public ClassLoader getClassLoader() {
            return delegate.getClassLoader();
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(byte[] data) {
            return delegate.createObjectDataInput(data);
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(byte[] data, int offset) {
            return delegate.createObjectDataInput(data, offset);
        }

        @Override
        public BufferObjectDataInput createObjectDataInput(Data data) {
            return delegate.createObjectDataInput(data);
        }

        @Override
        public BufferObjectDataOutput createObjectDataOutput(int size) {
            return delegate.createObjectDataOutput(size);
        }

        @Override
        public BufferObjectDataOutput createObjectDataOutput(int initialSize, int firstGrowthSize) {
            return delegate.createObjectDataOutput(initialSize, firstGrowthSize);
        }

        @Override
        public ManagedContext getManagedContext() {
            return delegate.getManagedContext();
        }

        @Override
        public <B extends Data> B trimSchema(Data data) {
            return delegate.trimSchema(data);
        }

        @Override
        public Schema extractSchemaFromData(@Nonnull Data data) throws IOException {
            return delegate.extractSchemaFromData(data);
        }

        @Override
        public Schema extractSchemaFromObject(@Nonnull Object object) {
            return delegate.extractSchemaFromObject(object);
        }

        @Override
        public boolean isCompactSerializable(Object object) {
            return delegate.isCompactSerializable(object);
        }

        @Override
        public PortableContext getPortableContext() {
            return delegate.getPortableContext();
        }

        @Override
        public void disposeData(Data data) {
            delegate.disposeData(data);
        }

        @Override
        public BufferObjectDataOutput createObjectDataOutput() {
            return delegate.createObjectDataOutput();
        }

        @Override
        public ByteOrder getByteOrder() {
            return delegate.getByteOrder();
        }

        @Override
        public byte getVersion() {
            return delegate.getVersion();
        }

        @Override
        public CompactStreamSerializer getCompactStreamSerializer() {
            return delegate.getCompactStreamSerializer();
        }

        @Override
        public PortableSerializer getPortableSerializer() {
            return delegate.getPortableSerializer();
        }

        @Override
        public void dispose() {
            delegate.dispose();
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
            return new MockSerializationService(super.createSerializationService());
        }
    }

}
