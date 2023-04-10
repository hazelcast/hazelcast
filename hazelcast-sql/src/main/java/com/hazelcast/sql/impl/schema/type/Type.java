/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.schema.type;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.jet.sql.impl.parse.SqlCreateType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A class stored in the SQL catalog to represent a type created using the
 * CREATE TYPE command.
 * <p>
 * It can represent a java class, or a portable/compact type, see {@link #kind}.
 */
public class Type implements Serializable, SqlCatalogObject {
    private String name;
    private TypeKind kind = TypeKind.JAVA;
    private String javaClassName;
    private String compactTypeName;
    private Integer portableFactoryId;
    private Integer portableClassId;
    private Integer portableVersion;
    private List<TypeField> fields;

    public Type() {
    }

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

    public Integer getPortableFactoryId() {
        return portableFactoryId;
    }

    public void setPortableFactoryId(final Integer portableFactoryId) {
        this.portableFactoryId = portableFactoryId;
    }

    public Integer getPortableClassId() {
        return portableClassId;
    }

    public void setPortableClassId(final Integer portableClassId) {
        this.portableClassId = portableClassId;
    }

    public Integer getPortableVersion() {
        return portableVersion;
    }

    public void setPortableVersion(final Integer portableVersion) {
        this.portableVersion = portableVersion;
    }

    public String getCompactTypeName() {
        return compactTypeName;
    }

    public void setCompactTypeName(final String compactTypeName) {
        this.compactTypeName = compactTypeName;
    }

    public Map<String, String> options() {
        if (javaClassName != null) {
            return ImmutableMap.of(
                    "format", "java",
                    "javaClass", javaClassName);
        }

        if (compactTypeName != null) {
            return ImmutableMap.of(
                    "format", "compact",
                    "compactTypeName", compactTypeName);
        }

        if (portableFactoryId != null) {
            return ImmutableMap.of(
                    "format", "portable",
                    "portableFactoryId", String.valueOf(portableFactoryId),
                    "portableClassId", String.valueOf(portableClassId),
                    "portableClassVersion", String.valueOf(portableVersion != null ? portableVersion : 0));
        }

        throw new AssertionError("unexpected state");
    }

    @Override
    @Nonnull
    public String unparse() {
        return SqlCreateType.unparse(this);
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
                out.writeInt(portableFactoryId);
                out.writeInt(portableClassId);
                out.writeInt(portableVersion);
                break;
            case COMPACT:
                out.writeString(compactTypeName);
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
                this.portableFactoryId = in.readInt();
                this.portableClassId = in.readInt();
                this.portableVersion = in.readInt();
                break;
            case COMPACT:
                this.compactTypeName = in.readString();
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
    public int getClassId() {
        return SqlDataSerializerHook.TYPE;
    }

    public static class TypeField implements IdentifiedDataSerializable, Serializable {
        private String name;
        private QueryDataType queryDataType;

        public TypeField() {
        }

        public TypeField(final String name, final QueryDataType queryDataType) {
            this.name = name;
            this.queryDataType = queryDataType;
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

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(queryDataType == null ? -1 : queryDataType.getConverter().getId());
            out.writeString(queryDataType == null ? "" : queryDataType.getObjectTypeName());
        }

        @Override
        public void readData(final ObjectDataInput in) throws IOException {
            this.name = in.readString();
            final int converterId = in.readInt();
            final String typeName = in.readString();
            final Converter converter = Converters.getConverter(converterId);

            // TODO: is this the correct type kind? (NONE). Maybe worth writing it too.
            this.queryDataType = converter.getTypeFamily().equals(QueryDataTypeFamily.OBJECT)
                    && ((typeName != null && !typeName.isEmpty()))
                    ? new QueryDataType(typeName)
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
