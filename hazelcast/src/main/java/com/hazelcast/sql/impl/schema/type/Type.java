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

package com.hazelcast.sql.impl.schema.type;

import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Type implements IdentifiedDataSerializable, Serializable {
    private String name;
    private TypeKind kind = TypeKind.JAVA;
    private String javaClassName;
    private ClassDefinition portableClassDef;
    private Schema compactSchema;
    private List<TypeField> fields;

    public Type() { }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public TypeKind getKind() {
        return kind;
    }

    public void setKind(final TypeKind kind) {
        this.kind = kind;
    }

    public String getJavaClassName() {
        return javaClassName;
    }

    public void setJavaClassName(final String javaClassName) {
        this.javaClassName = javaClassName;
    }

    public List<TypeField> getFields() {
        return fields;
    }

    public void setFields(final List<TypeField> fields) {
        this.fields = fields;
    }

    public QueryDataType toQueryDataTypeRef() {
        switch (kind) {
            case JAVA:
                return new QueryDataType(name, javaClassName);
            case PORTABLE:
            case COMPACT:
                return new QueryDataType(name);
            default:
                throw new UnsupportedOperationException("Not implemented yet.");
        }
    }

    public ClassDefinition getPortableClassDef() {
        return portableClassDef;
    }

    public void setPortableClassDef(final ClassDefinition portableClassDef) {
        this.portableClassDef = portableClassDef;
    }

    public Schema getCompactSchema() {
        return compactSchema;
    }

    public void setCompactSchema(final Schema compactSchema) {
        this.compactSchema = compactSchema;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(kind.ordinal());
        switch (kind) {
            case JAVA:
                out.writeString(javaClassName);
                break;
            case PORTABLE:
                out.writeObject(portableClassDef);
                break;
            case COMPACT:
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Type Kind: " + kind);
        }

        out.writeInt(fields.size());
        for (final TypeField field : fields) {
            out.writeObject(field);
        }
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        this.name = in.readString();
        this.kind = TypeKind.values()[in.readInt()];
        switch (kind) {
            case JAVA:
                this.javaClassName = in.readString();
                break;
            case PORTABLE:
                this.portableClassDef = in.readObject();
                break;
            case COMPACT:
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Type Kind: " + kind);
        }

        final int size = in.readInt();
        this.fields = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            this.fields.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TYPE;
    }

    public static class TypeField implements IdentifiedDataSerializable, Serializable {
        private String name;
        private QueryDataType queryDataType;
        private String queryDataTypeMetadata = "";

        public TypeField() { }

        public TypeField(final String name, final QueryDataType queryDataType) {
            this.name = name;
            this.queryDataType = queryDataType;
        }

        public TypeField(final String name, final String queryDataTypeMetadata) {
            this.name = name;
            this.queryDataTypeMetadata = queryDataTypeMetadata;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public QueryDataType getQueryDataType() {
            return queryDataType;
        }

        public void setQueryDataType(final QueryDataType queryDataType) {
            this.queryDataType = queryDataType;
        }

        public String getQueryDataTypeMetadata() {
            return queryDataTypeMetadata;
        }

        public void setQueryDataTypeMetadata(final String queryDataTypeMetadata) {
            this.queryDataTypeMetadata = queryDataTypeMetadata;
        }

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(queryDataType == null ? -1 : queryDataType.getConverter().getId());
            out.writeString(queryDataType == null ? "" : queryDataType.getObjectTypeName());
            out.writeString(queryDataTypeMetadata);
        }

        @Override
        public void readData(final ObjectDataInput in) throws IOException {
            this.name = in.readString();
            final int converterId = in.readInt();
            final String typeName = in.readString();
            this.queryDataTypeMetadata = in.readString();

            // Type doesn't have a QueryDataType yet because its a class.
            // TODO: maybe empty HZ_OBJECT?
            if (converterId == -1) {
                return;
            }

            final Converter converter = Converters.getConverter(converterId);
            this.queryDataType = converter.getTypeFamily().equals(QueryDataTypeFamily.OBJECT)
                    && ((typeName != null && !typeName.isEmpty()) || queryDataTypeMetadata != null
                    && !queryDataTypeMetadata.isEmpty())
                    ? new QueryDataType(typeName, this.queryDataTypeMetadata)
                    : QueryDataTypeUtils.resolveTypeForClass(converter.getValueClass());
        }

        @Override
        public int getFactoryId() {
            return SqlDataSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return SqlDataSerializerHook.TYPE_FIELD;
        }
    }
}
