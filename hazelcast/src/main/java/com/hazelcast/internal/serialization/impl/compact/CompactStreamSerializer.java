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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.record.JavaRecordSerializer;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.serialization.impl.FieldOperations.fieldOperations;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.TYPE_COMPACT;

/**
 * Serializer for compact serializable objects.
 * <p>
 * This can be used to serialize objects that either
 * <ul>
 *     <li>registered as compact serializable through configuration.</li>
 *     <li>cannot be serialized with any other mechanism and they can be serialized
 *     reflectively, without any configuration.</li>
 * </ul>
 */
public class CompactStreamSerializer implements StreamSerializer<Object> {
    private final Map<Class, CompactSerializableRegistration> classToRegistrationMap = new ConcurrentHashMap<>();
    private final Map<String, CompactSerializableRegistration> typeNameToRegistrationMap = new ConcurrentHashMap<>();
    private final Map<Class, Schema> classToSchemaMap = new ConcurrentHashMap<>();
    private final ReflectiveCompactSerializer reflectiveSerializer = new ReflectiveCompactSerializer();
    private final JavaRecordSerializer javaRecordSerializer = new JavaRecordSerializer();
    private final SchemaService schemaService;
    private final ManagedContext managedContext;
    private final ClassLoader classLoader;
    //Should be deleted with removing Beta tags
    private final boolean isEnabled;

    public CompactStreamSerializer(CompactSerializationConfig compactSerializationConfig,
                                   ManagedContext managedContext, SchemaService schemaService,
                                   ClassLoader classLoader) {
        this.managedContext = managedContext;
        this.schemaService = schemaService;
        this.classLoader = classLoader;
        this.isEnabled = compactSerializationConfig.isEnabled();
        registerConfiguredSerializers(compactSerializationConfig);
        registerConfiguredNamedSerializers(compactSerializationConfig);
    }

    /**
     * Returns true if the {@code clazz} is registered as compact serializable
     * through configuration.
     */
    public boolean isRegisteredAsCompact(Class clazz) {
        return classToRegistrationMap.containsKey(clazz);
    }

    @Override
    public int getTypeId() {
        return TYPE_COMPACT;
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
        putToSchemaService(includeSchemaOnBinary, schema);
        writeSchema(output, includeSchemaOnBinary, schema);
        DefaultCompactWriter writer = new DefaultCompactWriter(this, output, schema, includeSchemaOnBinary);
        Collection<FieldDescriptor> fields = schema.getFields();
        for (FieldDescriptor fieldDescriptor : fields) {
            String fieldName = fieldDescriptor.getFieldName();
            FieldKind fieldKind = fieldDescriptor.getKind();
            fieldOperations(fieldKind).writeFieldFromRecordToWriter(writer, record, fieldName);
        }
        writer.end();
    }

    private void putToSchemaService(boolean includeSchemaOnBinary, Schema schema) {
        if (includeSchemaOnBinary) {
            //if we will include the schema on binary, the schema will be delivered anyway.
            //No need to put it to cluster. Putting it local only in order not to ask from remote on read.
            schemaService.putLocal(schema);
        } else {
            schemaService.put(schema);
        }
    }

    public void writeObject(BufferObjectDataOutput out, Object o, boolean includeSchemaOnBinary) throws IOException {
        Class<?> aClass = o.getClass();
        CompactSerializableRegistration registration = getOrCreateRegistration(aClass);

        Schema schema = classToSchemaMap.get(aClass);
        if (schema == null) {
            schema = buildSchema(registration, o);
            putToSchemaService(includeSchemaOnBinary, schema);
            classToSchemaMap.put(aClass, schema);
        }
        writeSchema(out, includeSchemaOnBinary, schema);
        DefaultCompactWriter writer = new DefaultCompactWriter(this, out, schema, includeSchemaOnBinary);
        registration.getSerializer().write(writer, o);
        writer.end();
    }

    private void writeSchema(BufferObjectDataOutput out, boolean includeSchemaOnBinary, Schema schema) throws IOException {
        out.writeLong(schema.getSchemaId());
        if (includeSchemaOnBinary) {
            int sizeOfSchemaPosition = out.position();
            out.writeInt(0);
            int schemaBeginPos = out.position();
            out.writeObject(schema);
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
        CompactSerializableRegistration registration = getOrCreateRegistration(schema.getTypeName());

        if (registration == null) {
            //we have tried to load class via class loader, it did not work. We are returning a GenericRecord.
            return readGenericRecord(input, schema, schemaIncludedInBinary);
        }

        DefaultCompactReader reader = new DefaultCompactReader(this, input, schema,
                registration.getClazz(), schemaIncludedInBinary);
        Object object = registration.getSerializer().read(reader);
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
            schema = input.readObject();
            long incomingSchemaId = schema.getSchemaId();
            if (schemaId != incomingSchemaId) {
                throw new HazelcastSerializationException("Invalid schema id found. Expected " + schemaId
                        + ", actual " + incomingSchemaId + " for schema " + schema);
            }
            schemaService.putLocal(schema);
            return schema;
        }
        throw new HazelcastSerializationException("The schema can not be found with id " + schemaId);
    }

    private CompactSerializableRegistration getOrCreateRegistration(Class clazz) {
        return classToRegistrationMap.computeIfAbsent(clazz, aClass -> {
            CompactSerializer serializer = javaRecordSerializer.isRecord(aClass) ? javaRecordSerializer : reflectiveSerializer;
            return new CompactSerializableRegistration(aClass, aClass.getName(), serializer);
        });
    }

    private CompactSerializableRegistration getOrCreateRegistration(String typeName) {
        return typeNameToRegistrationMap.computeIfAbsent(typeName, s -> {
            Class<?> clazz;
            try {
                //when the registration does not exist, we treat typeName as className to check if there is a class
                //with the given name in the classpath.
                clazz = ClassLoaderUtil.loadClass(classLoader, typeName);
            } catch (Exception e) {
                return null;
            }
            try {
                return getOrCreateRegistration(clazz);
            } catch (Exception e) {
                throw new HazelcastSerializationException("Class " + clazz + " must have an empty constructor", e);
            }
        });
    }

    private GenericRecord readGenericRecord(BufferObjectDataInput input, Schema schema, boolean schemaIncludedInBinary) {
        CompactInternalGenericRecord record =
                new CompactInternalGenericRecord(this, input, schema, null, schemaIncludedInBinary);
        Collection<FieldDescriptor> fields = schema.getFields();
        DeserializedSchemaBoundGenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        for (FieldDescriptor fieldDescriptor : fields) {
            String fieldName = fieldDescriptor.getFieldName();
            FieldKind fieldKind = fieldDescriptor.getKind();
            builder.write(fieldName, record.readAny(fieldName), fieldKind);
        }
        return builder.build();
    }

    public GenericRecord readGenericRecord(ObjectDataInput in, boolean schemaIncludedInBinary) throws IOException {
        Schema schema = getOrReadSchema(in, schemaIncludedInBinary);
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return readGenericRecord(input, schema, schemaIncludedInBinary);
    }

    public InternalGenericRecord readAsInternalGenericRecord(ObjectDataInput in) throws IOException {
        Schema schema = getOrReadSchema(in, false);
        BufferObjectDataInput input = (BufferObjectDataInput) in;
        return new CompactInternalGenericRecord(this, input, schema, null, false);
    }

    //Should be deleted with removing Beta tags
    public boolean isEnabled() {
        return isEnabled;
    }

    private void registerConfiguredSerializers(CompactSerializationConfig compactSerializationConfig) {
        Map<String, TriTuple<Class, String, CompactSerializer>> registrations
                = CompactSerializationConfigAccessor.getRegistrations(compactSerializationConfig);
        for (TriTuple<Class, String, CompactSerializer> registration : registrations.values()) {
            Class clazz = registration.element1;
            String typeName = registration.element2;
            CompactSerializer serializer = registration.element3;
            if (serializer == null) {
                if (javaRecordSerializer.isRecord(clazz)) {
                    serializer = javaRecordSerializer;
                } else {
                    serializer = reflectiveSerializer;
                }
            }
            CompactSerializableRegistration serializableRegistration
                    = new CompactSerializableRegistration(clazz, typeName, serializer);
            classToRegistrationMap.put(clazz, serializableRegistration);
            typeNameToRegistrationMap.put(typeName, serializableRegistration);
        }
    }

    private void registerConfiguredNamedSerializers(CompactSerializationConfig compactSerializationConfig) {
        Map<String, TriTuple<String, String, String>> namedRegistries
                = CompactSerializationConfigAccessor.getNamedRegistrations(compactSerializationConfig);
        for (TriTuple<String, String, String> registry : namedRegistries.values()) {
            String className = registry.element1;
            String typeName = registry.element2;
            String serializerClassName = registry.element3;

            Class clazz;
            try {
                clazz = ClassLoaderUtil.loadClass(classLoader, className);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Cannot load the class " + className);
            }

            CompactSerializer serializer;
            if (serializerClassName != null) {
                try {
                    serializer = ClassLoaderUtil.newInstance(classLoader, serializerClassName);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Cannot create an instance of " + serializerClassName);
                }
            } else {
                if (javaRecordSerializer.isRecord(clazz)) {
                    serializer = javaRecordSerializer;
                } else {
                    serializer = reflectiveSerializer;
                }
            }

            CompactSerializableRegistration registration = new CompactSerializableRegistration(clazz, typeName, serializer);
            classToRegistrationMap.put(clazz, registration);
            typeNameToRegistrationMap.put(typeName, registration);
        }
    }

    public Schema extractSchema(BufferObjectDataInput objectDataInput) throws IOException {
        return getOrReadSchema(objectDataInput, false);
    }

    public Schema extractSchema(Object o) {
        Class<?> aClass = o.getClass();

        Schema schema = classToSchemaMap.get(aClass);
        if (schema == null) {
            CompactSerializableRegistration registration = getOrCreateRegistration(aClass);
            schema = buildSchema(registration, o);
            schemaService.putLocal(schema);
            classToSchemaMap.put(aClass, schema);
            return schema;
        }
        return schema;
    }

    private static Schema buildSchema(CompactSerializableRegistration registration, Object o) {
        SchemaWriter writer = new SchemaWriter(registration.getTypeName());
        registration.getSerializer().write(writer, o);
        return writer.build();
    }
}
