/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.sql.impl.QueryException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.sql.impl.schema.map.options.PortableMapOptionsMetadataResolver.lookupClassDefinition;
import static java.lang.String.format;

// TODO: can it be non-thread safe ?
public class PortableUpsertTarget implements UpsertTarget {

    private final ClassDefinition classDefinition;

    private GenericPortable portable;

    PortableUpsertTarget(
            InternalSerializationService serializationService,
            int factoryId, int classId, int classVersion
    ) {
        this.classDefinition = lookupClassDefinition(serializationService, factoryId, classId, classVersion);
    }

    @Override
    public UpsertInjector createInjector(String path) {
        FieldDefinition fieldDefinition = checkNotNull(classDefinition.getField(path), "Missing field");
        return value -> portable.add(fieldDefinition, value);
    }

    @Override
    public void init() {
        portable = new GenericPortable();
    }

    @Override
    public Object conclude() {
        GenericPortable portable = this.portable;
        this.portable = null;
        return portable;
    }

    // TODO: replace with GenericRecord when available
    private final class GenericPortable implements VersionedPortable {

        private final List<FieldDefinition> fieldDefinitions;
        private final List<Object> values;

        private GenericPortable() {
            this.fieldDefinitions = new ArrayList<>();
            this.values = new ArrayList<>();
        }

        private void add(FieldDefinition fieldDefinition, Object value) {
            fieldDefinitions.add(fieldDefinition);
            values.add(value);
        }

        @Override
        public int getFactoryId() {
            return classDefinition.getFactoryId();
        }

        @Override
        public int getClassId() {
            return classDefinition.getClassId();
        }

        @Override
        public int getClassVersion() {
            return classDefinition.getVersion();
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            for (int i = 0; i < fieldDefinitions.size(); i++) {
                FieldDefinition fieldDefinition = fieldDefinitions.get(i);
                Object value = values.get(i);
                write(writer, fieldDefinition, value);
            }
        }

        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        private void write(PortableWriter writer, FieldDefinition fieldDefinition, Object value) throws IOException {
            // TODO: temporal data types, BigDecimal, BigInteger - extend Portable supported types set ???
            String name = fieldDefinition.getName();
            FieldType type = fieldDefinition.getType();
            switch (type) {
                case BOOLEAN:
                    writer.writeBoolean(name, value != null && (boolean) value);
                    break;
                case BYTE:
                    writer.writeByte(name, value == null ? (byte) 0 : (byte) value);
                    break;
                case SHORT:
                    writer.writeShort(name, value == null ? (short) 0 : (short) value);
                    break;
                case CHAR:
                    writer.writeChar(name, value == null ? (char) 0 : (char) value);
                    break;
                case INT:
                    writer.writeInt(name, value == null ? 0 : (int) value);
                    break;
                case LONG:
                    writer.writeLong(name, value == null ? 0L : (long) value);
                    break;
                case FLOAT:
                    writer.writeFloat(name, value == null ? 0F : (float) value);
                    break;
                case DOUBLE:
                    writer.writeDouble(name, value == null ? 0D : (double) value);
                    break;
                case UTF:
                    writer.writeUTF(name, (String) value);
                    break;
                default:
                    throw QueryException.dataException(
                            format("Unsupported type - %s", type.name())
                    );
            }
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            throw new UnsupportedEncodingException();
        }
    }
}
