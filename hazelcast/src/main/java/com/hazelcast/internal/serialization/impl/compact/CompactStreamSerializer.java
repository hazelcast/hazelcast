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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.CompactSerializationConfigAccessor;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.impl.AbstractSerializationService;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.record.JavaRecordSerializer;
import com.hazelcast.internal.util.TriTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
    private final ReflectiveCompactSerializer reflectiveSerializer = new ReflectiveCompactSerializer(this);
    private final JavaRecordSerializer javaRecordSerializer = new JavaRecordSerializer(this);
    private final SchemaService schemaService;
    private final ManagedContext managedContext;
    private final ClassLoader classLoader;
    private final AbstractSerializationService serializationService;

    public CompactStreamSerializer(AbstractSerializationService serializationService,
                                   CompactSerializationConfig compactSerializationConfig,
                                   ManagedContext managedContext, SchemaService schemaService,
                                   ClassLoader classLoader) {
        this.serializationService = serializationService;
        this.managedContext = managedContext;
        this.schemaService = schemaService;
        this.classLoader = classLoader;
        registerSerializers(compactSerializationConfig);
        registerDeclarativeConfigSerializers(compactSerializationConfig);
        registerDeclarativeConfigClasses(compactSerializationConfig);
    }

    /**
     * Returns true if the {@code clazz} is registered as compact serializable
     * through configuration.
     */
    public boolean isRegisteredAsCompact(Class clazz) {
        return classToRegistrationMap.containsKey(clazz);
    }

    public Collection<Class> getCompactSerializableClasses() {
        return classToRegistrationMap.keySet();
    }

    public boolean canBeSerializedAsCompact(Class<?> clazz) {
        return serializationService.serializerForClass(clazz, false) instanceof CompactStreamSerializerAdapter;
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
        if (!includeSchemaOnBinary) {
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

        if (registration == CompactSerializableRegistration.GENERIC_RECORD_REGISTRATION) {
            // We have tried to load class via class loader, it did not work.
            // We are returning a GenericRecord.
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
        CompactSerializableRegistration currentRegistration = typeNameToRegistrationMap.get(typeName);
        if (currentRegistration != null) {
            return currentRegistration;
        }
        // Execute potentially long-lasting operation outside CHM lock in computeIfAbsent.
        // Some special classloaders (eg. JetClassLoader) may try to access external resources
        // and require other threads.
        // We might try to load the same class multiple times in parallel but this is not a problem.
        CompactSerializableRegistration newRegistration = getOrCreateRegistration0(typeName);

        // Registration might have been created by a concurrent thread.
        // If so, use that one instead.
        return typeNameToRegistrationMap.computeIfAbsent(typeName, k -> newRegistration);
    }

    private CompactSerializableRegistration getOrCreateRegistration0(String typeName) {
        Class<?> clazz;
        try {
            // When the registration does not exist, we treat typeName as className
            // to check if there is a class with the given name in the classpath.
            clazz = ClassLoaderUtil.loadClass(classLoader, typeName);
        } catch (Exception e) {
            // There is no such class that has typeName as its name.
            // We should try to read this as GenericRecord. We are
            // returning this registration here to remember that we
            // should read instances of this typeName as GenericRecords,
            // instead of trying to load a class with that name over
            // and over.
            return CompactSerializableRegistration.GENERIC_RECORD_REGISTRATION;
        }

        return getOrCreateRegistration(clazz);
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

    private void registerSerializers(CompactSerializationConfig compactSerializationConfig) {
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
            saveRegistration(serializableRegistration);
        }
    }

    private void saveRegistration(CompactSerializableRegistration registration) {
        Class clazz = registration.getClazz();
        CompactSerializableRegistration existing = classToRegistrationMap.putIfAbsent(clazz, registration);
        if (existing != null) {
            throw new InvalidConfigurationException("Duplicate serializer registrations "
                    + "are found for the class '" + clazz + "'. Make sure only one "
                    + "Compact serializer is registered for the same class. "
                    + "Existing serializer: " + existing.getSerializer() + ", "
                    + "new serializer: " + registration.getSerializer());
        }

        String typeName = registration.getTypeName();
        existing = typeNameToRegistrationMap.putIfAbsent(typeName, registration);
        if (existing != null) {
            throw new InvalidConfigurationException("Duplicate serializer registrations "
                    + "are found for the type name '" + typeName + "'. Make sure only one "
                    + "Compact serializer is registered for the same type name. "
                    + "Existing serializer: " + existing.getSerializer() + ", "
                    + "new serializer: " + registration.getSerializer());
        }
    }

    private void registerDeclarativeConfigSerializers(CompactSerializationConfig config) {
        List<String> serializerClassNames = CompactSerializationConfigAccessor.getSerializerClassNames(config);
        for (String className : serializerClassNames) {
            CompactSerializer serializer;
            try {
                serializer = ClassLoaderUtil.newInstance(classLoader, className);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Cannot create an instance "
                        + "of the Compact serializer '" + className + "'.");
            }

            CompactSerializableRegistration registration = new CompactSerializableRegistration(
                    serializer.getCompactClass(),
                    serializer.getTypeName(),
                    serializer
            );

            saveRegistration(registration);
        }
    }

    private void registerDeclarativeConfigClasses(CompactSerializationConfig config) {
        List<String> compactSerializableClassNames
                = CompactSerializationConfigAccessor.getCompactSerializableClassNames(config);
        for (String className : compactSerializableClassNames) {
            Class clazz;
            try {
                clazz = ClassLoaderUtil.loadClass(classLoader, className);
            } catch (ClassNotFoundException e) {
                throw new InvalidConfigurationException("Cannot load the Compact "
                        + "serializable class '" + className + "'.");
            }

            CompactSerializer serializer;
            if (javaRecordSerializer.isRecord(clazz)) {
                serializer = javaRecordSerializer;
            } else {
                serializer = reflectiveSerializer;
            }

            CompactSerializableRegistration registration = new CompactSerializableRegistration(
                    clazz,
                    className,
                    serializer
            );

            saveRegistration(registration);
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
