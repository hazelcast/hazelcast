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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.TYPE_COMPACT;

public class CompactStreamSerializer implements StreamSerializer<Object> {
    private final boolean isDebug = System.getProperty("com.hazelcast.serialization.compact.debug") != null;
    private final Map<Class, ConfigurationRegistry> classToRegistryMap = new ConcurrentHashMap<>();
    private final Map<String, ConfigurationRegistry> classNameToRegistryMap = new ConcurrentHashMap<>();

    private final Map<Class, Schema> classToSchemaMap = new ConcurrentHashMap<>();
    private final ReflectiveCompactSerializer reflectiveSerializer = new ReflectiveCompactSerializer();
    private final SchemaService schemaService;
    private final ManagedContext managedContext;
    //TODO sancar cleanup by solving enterprise integration better. We should not need all the serialization servie
    private final InternalSerializationService internalSerializationService;


    public CompactStreamSerializer(CompactSerializationConfig compactSerializationConfig,
                                   InternalSerializationService internalSerializationService,
                                   ManagedContext managedContext, SchemaService schemaService) {
        this.managedContext = managedContext;
        this.internalSerializationService = internalSerializationService;
        this.schemaService = schemaService;
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

    public InternalSerializationService getInternalSerializationService() {
        return internalSerializationService;
    }

    public InternalGenericRecord readAsInternalGenericRecord(ObjectDataInput input) throws IOException {
        return (InternalGenericRecord) readGenericRecord(input);
    }

    @Override
    public int getTypeId() {
        return TYPE_COMPACT;
    }

    public GenericRecordBuilder createGenericRecordBuilder(Schema schema) {
        return new SerializingGenericRecordBuilder(this, schema,
                internalSerializationService::createObjectDataInput,
                internalSerializationService::createObjectDataOutput);
    }

    //========================== WRITE =============================//
    @Override
    public void write(ObjectDataOutput out, Object o) throws IOException {
        assert out instanceof BufferObjectDataOutput;
        BufferObjectDataOutput bufferObjectDataOutput = (BufferObjectDataOutput) out;
        if (o instanceof GenericRecord) {
            writeGenericRecord(bufferObjectDataOutput, (GenericRecord) o);
        } else {
            writeObject(bufferObjectDataOutput, o);
        }
    }

    void writeGenericRecord(BufferObjectDataOutput out, GenericRecord o) throws IOException {
        writeCompactGenericRecord(out, (CompactGenericRecord) o);
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity"})
    void writeCompactGenericRecord(BufferObjectDataOutput output, CompactGenericRecord record) throws IOException {
        Schema schema = record.getSchema();
        if (!schema.isSchemaIdSet()) {
            Data data = internalSerializationService.toData(schema);
            long schemaId = RabinFingerPrint.fingerprint64(data.toByteArray());
            schema.setSchemaId(schemaId);
        }
        schemaService.put(schema);
        if (isDebug) {
            System.out.println("DEBUG WRITE DESERIALIZED GENERIC RECORD " + schema.getTypeName() + " pos "
                    + output.position() + " " + schema.getSchemaId());
        }
        output.writeLong(schema.getSchemaId());
        DefaultCompactWriter writer = new DefaultCompactWriter(this, output, schema);
        Collection<FieldDescriptor> fields = schema.getFields();
        for (FieldDescriptor fieldDescriptor : fields) {
            String fieldName = fieldDescriptor.getFieldName();
            FieldType fieldType = fieldDescriptor.getType();
            fieldOperations(fieldType).readFromGenericRecordToWriter(writer, record, fieldName);
        }
        writer.end();
    }

    public void writeObject(BufferObjectDataOutput out, Object o) throws IOException {
        ConfigurationRegistry registry = getOrCreateRegistry(o);
        Class<?> aClass = o.getClass();

        Schema schema = classToSchemaMap.get(aClass);
        if (schema == null) {
            SchemaWriter writer = new SchemaWriter(registry.getTypeName());
            registry.getSerializer().write(writer, o);
            schema = writer.build();
            Data data = internalSerializationService.toData(schema);
            long schemaId = RabinFingerPrint.fingerprint64(data.toByteArray());
            schema.setSchemaId(schemaId);
            schemaService.put(schema);
            classToSchemaMap.put(aClass, schema);
        }
        if (isDebug) {
            System.out.println("DEBUG WRITE OBJECT " + schema.getTypeName() + " pos "
                    + out.position() + " " + schema.getSchemaId());
        }
        out.writeLong(schema.getSchemaId());
        DefaultCompactWriter writer = new DefaultCompactWriter(this, out, schema);
        registry.getSerializer().write(writer, o);
        writer.end();
    }


    //========================== READ =============================//

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        if (isDebug) {
            System.out.print("DEBUG READ pos " + input.position());
        }
        long schemaId = input.readLong();
        if (isDebug) {
            System.out.println(" schemaId " + schemaId);
        }
        Schema schema = schemaService.get(schemaId);
        if (isDebug) {
            System.out.println("DEBUG READ schema class name " + schema.getTypeName());
        }
        ConfigurationRegistry registry = getOrCreateRegistry(schema.getTypeName());

        if (registry == null) {
            //we have tried to load class via class loader, it did not work. We are returning a GenericRecord.
            return new DefaultCompactReader(this, input, schema, null);
        }

        DefaultCompactReader genericRecord = new DefaultCompactReader(this, input, schema, registry.getClazz());
        Object object = registry.getSerializer().read(genericRecord);
        return managedContext != null ? managedContext.initialize(object) : object;

    }

    public <T> T readObject(ObjectDataInput in) throws IOException {
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        long schemaId = input.readLong();
        Schema schema = schemaService.get(schemaId);
        ConfigurationRegistry registry = getOrCreateRegistry(schema.getTypeName());

        if (registry == null) {
            throw new HazelcastSerializationException("The class should be in the classpath to be read via readObject* methods."
                    + "Associated schema for the data : " + schema);
        }

        DefaultCompactReader genericRecord = new DefaultCompactReader(this, input, schema, registry.getClazz());
        Object object = registry.getSerializer().read(genericRecord);
        return managedContext != null ? (T) managedContext.initialize(object) : (T) object;
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
                clazz = ClassLoaderUtil.loadClass(internalSerializationService.getClassLoader(), className);
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

    public GenericRecord readGenericRecord(ObjectDataInput in) throws IOException {
        long schemaId = in.readLong();
        Schema schema = schemaService.get(schemaId);
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return new DefaultCompactReader(this, input, schema, null);
    }
}
