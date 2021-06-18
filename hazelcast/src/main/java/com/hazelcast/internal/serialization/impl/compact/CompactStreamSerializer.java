/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.TYPE_COMPACT;

public class CompactStreamSerializer implements StreamSerializer<Object> {
    private final Map<Class, ConfigurationRegistry> classToRegistryMap = new ConcurrentHashMap<>();
    private final Map<String, ConfigurationRegistry> classNameToRegistryMap = new ConcurrentHashMap<>();
    private final Map<Class, Schema> classToSchemaMap = new ConcurrentHashMap<>();
    private final ReflectiveCompactSerializer reflectiveSerializer = new ReflectiveCompactSerializer();
    private final SchemaService schemaService;
    private final ManagedContext managedContext;
    private final ClassLoader classLoader;
    private final Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc;
    private final Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier;

    public CompactStreamSerializer(CompactSerializationConfig compactSerializationConfig,
                                   ManagedContext managedContext, SchemaService schemaService,
                                   ClassLoader classLoader,
                                   Function<byte[], BufferObjectDataInput> bufferObjectDataInputFunc,
                                   Supplier<BufferObjectDataOutput> bufferObjectDataOutputSupplier) {
        this.managedContext = managedContext;
        this.schemaService = schemaService;
        this.bufferObjectDataInputFunc = bufferObjectDataInputFunc;
        this.bufferObjectDataOutputSupplier = bufferObjectDataOutputSupplier;
        this.classLoader = classLoader;
        Map<String, TriTuple<Class, String, CompactSerializer>> registries = compactSerializationConfig.getRegistries();
        for (Map.Entry<String, TriTuple<Class, String, CompactSerializer>> entry : registries.entrySet()) {
            String typeName = entry.getKey();
            CompactSerializer serializer = entry.getValue().element3;
            InternalCompactSerializer compactSerializer = serializer == null ? reflectiveSerializer : serializer;
            Class clazz = entry.getValue().element1;
            classToRegistryMap.put(clazz, new ConfigurationRegistry(clazz, typeName, compactSerializer));
            classNameToRegistryMap.put(typeName, new ConfigurationRegistry(clazz, typeName, compactSerializer));
        }
    }

    public InternalGenericRecord readAsInternalGenericRecord(ObjectDataInput input, boolean schemaIncludedInBinary)
            throws IOException {
        return (InternalGenericRecord) readGenericRecord(input, schemaIncludedInBinary);
    }

    @Override
    public int getTypeId() {
        return TYPE_COMPACT;
    }

    public GenericRecordBuilder createGenericRecordBuilder(Schema schema) {
        return new SerializingGenericRecordBuilder(this, schema,
                bufferObjectDataInputFunc,
                bufferObjectDataOutputSupplier);
    }

    public GenericRecordBuilder createGenericRecordCloner(Schema schema, CompactInternalGenericRecord record) {
        return new SerializingGenericRecordCloner(this, schema, record,
                bufferObjectDataInputFunc,
                bufferObjectDataOutputSupplier);
    }

    //========================== WRITE =============================//
    @Override
    public void write(ObjectDataOutput out, Object o) throws IOException {
        assert out instanceof BufferObjectDataOutput;
        BufferObjectDataOutput bufferObjectDataOutput = (BufferObjectDataOutput) out;
        write(bufferObjectDataOutput, o, false);
    }

    void write(BufferObjectDataOutput out, Object o, boolean includeSchemaOnBinary) throws IOException {
        if (o instanceof CompactGenericRecord) {
            writeGenericRecord(out, (CompactGenericRecord) o, includeSchemaOnBinary);
        } else {
            writeObject(out, o, includeSchemaOnBinary);
        }
    }

    void writeGenericRecord(BufferObjectDataOutput output, CompactGenericRecord record,
                            boolean includeSchemaOnBinary) throws IOException {
        Schema schema = record.getSchema();
        if (!schema.isSchemaIdSet()) {
            long schemaId = calculateSchemaId(schema);
            schema.setSchemaId(schemaId);
        }
        schemaService.put(schema);
        writeSchema(output, includeSchemaOnBinary, schema);
        DefaultCompactWriter writer = new DefaultCompactWriter(this, output, schema, includeSchemaOnBinary);
        Collection<FieldDescriptor> fields = schema.getFields();
        for (FieldDescriptor fieldDescriptor : fields) {
            String fieldName = fieldDescriptor.getFieldName();
            FieldType fieldType = fieldDescriptor.getType();
            fieldOperations(fieldType).readFromGenericRecordToWriter(writer, record, fieldName);
        }
        writer.end();
    }

    private long calculateSchemaId(Schema schema) throws IOException {
        BufferObjectDataOutput out = bufferObjectDataOutputSupplier.get();
        schema.writeData(out);
        return RabinFingerPrint.fingerprint64(out.toByteArray());
    }

    public void writeObject(BufferObjectDataOutput out, Object o, boolean includeSchemaOnBinary) throws IOException {
        ConfigurationRegistry registry = getOrCreateRegistry(o);
        Class<?> aClass = o.getClass();

        Schema schema = classToSchemaMap.get(aClass);
        if (schema == null) {
            SchemaWriter writer = new SchemaWriter(registry.getTypeName());
            registry.getSerializer().write(writer, o);
            schema = writer.build();
            long schemaId = calculateSchemaId(schema);
            schema.setSchemaId(schemaId);
            schemaService.put(schema);
            classToSchemaMap.put(aClass, schema);
        }
        writeSchema(out, includeSchemaOnBinary, schema);
        DefaultCompactWriter writer = new DefaultCompactWriter(this, out, schema, includeSchemaOnBinary);
        registry.getSerializer().write(writer, o);
        writer.end();
    }

    private void writeSchema(BufferObjectDataOutput out, boolean includeSchemaOnBinary, Schema schema) throws IOException {
        out.writeLong(schema.getSchemaId());
        if (includeSchemaOnBinary) {
            int sizeOfSchemaPosition = out.position();
            out.writeInt(0);
            int schemaBeginPos = out.position();
            schema.writeData(out);
            int schemaEndPosition = out.position();
            out.writeInt(sizeOfSchemaPosition, schemaEndPosition - schemaBeginPos);
        }
    }


    //========================== READ =============================//

    @Override
    public Object read(@Nonnull ObjectDataInput in) throws IOException {
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return read(input, false);
    }

    Object read(BufferObjectDataInput input, boolean schemaIncludedInBinary) throws IOException {
        Schema schema = getOrReadSchema(input, schemaIncludedInBinary);
        ConfigurationRegistry registry = getOrCreateRegistry(schema.getTypeName());

        if (registry == null) {
            //we have tried to load class via class loader, it did not work. We are returning a GenericRecord.
            return new DefaultCompactReader(this, input, schema, null, schemaIncludedInBinary);
        }

        DefaultCompactReader genericRecord = new DefaultCompactReader(this, input, schema,
                registry.getClazz(), schemaIncludedInBinary);
        Object object = registry.getSerializer().read(genericRecord);
        return managedContext != null ? managedContext.initialize(object) : object;

    }

    private Schema getOrReadSchema(ObjectDataInput input, boolean schemaIncludedInBinary) throws IOException {
        long schemaId = input.readLong();
        Schema schema = schemaService.get(schemaId);
        if (schema != null) {
            if (schemaIncludedInBinary) {
                int sizeOfSchema = input.readInt();
                input.skipBytes(sizeOfSchema);
            }
            return schema;
        }
        if (schemaIncludedInBinary) {
            //sizeOfSchema
            input.readInt();
            schema = new Schema();
            schema.readData(input);
            long includedSchemaId = calculateSchemaId(schema);
            if (schemaId != includedSchemaId) {
                throw new HazelcastSerializationException("Invalid schema id found. Expected " + schemaId
                        + ", actual " + includedSchemaId + " for schema " + schema);
            }
            schema.setSchemaId(schemaId);
            schemaService.put(schema);
            return schema;
        }
        throw new HazelcastSerializationException("The schema can not be found with id " + schemaId);
    }

    private ConfigurationRegistry getOrCreateRegistry(Object object) {
        return classToRegistryMap.computeIfAbsent(object.getClass(), aClass -> {
            if (object instanceof Compactable) {
                CompactSerializer<?> serializer = ((Compactable<?>) object).getCompactSerializer();
                return new ConfigurationRegistry(aClass, aClass.getName(), serializer);
            }
            return new ConfigurationRegistry(aClass, aClass.getName(), reflectiveSerializer);
        });
    }

    private ConfigurationRegistry getOrCreateRegistry(String className) {
        return classNameToRegistryMap.computeIfAbsent(className, s -> {
            Class<?> clazz;
            try {
                clazz = ClassLoaderUtil.loadClass(classLoader, className);
            } catch (Exception e) {
                return null;
            }
            try {
                Object object = ClassLoaderUtil.newInstance(clazz.getClassLoader(), clazz);
                return getOrCreateRegistry(object);
            } catch (Exception e) {
                throw new HazelcastSerializationException("Class " + clazz + " must have an empty constructor", e);
            }
        });
    }

    public GenericRecord readGenericRecord(ObjectDataInput in, boolean schemaIncludedInBinary) throws IOException {
        Schema schema = getOrReadSchema(in, schemaIncludedInBinary);
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return new DefaultCompactReader(this, input, schema, null, schemaIncludedInBinary);
    }
}
