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

import com.hazelcast.jet.sql.impl.parse.SqlCreateType;
import com.hazelcast.jet.sql.impl.schema.TypeDefinitionColumn;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * A class stored in the SQL catalog to represent a type created using the
 * CREATE TYPE command.
 */
public class Type implements Serializable, SqlCatalogObject {
    private String name;
    private List<TypeField> fields;
    private Map<String, String> options;

    public Type() { }

    public Type(String name, List<TypeDefinitionColumn> columns, Map<String, String> options) {
        this.name = name;
        this.fields = columns.stream().map(column -> new TypeField(column.name(), column.dataType())).collect(toList());
        this.options = options;
    }

    @Override
    public String name() {
        return name;
    }

    public List<TypeField> getFields() {
        return fields;
    }

    public void setFields(List<TypeField> fields) {
        this.fields = fields;
    }

    public Map<String, String> options() {
        return Collections.unmodifiableMap(options);
    }

    @Override
    @Nonnull
    public String unparse() {
        return SqlCreateType.unparse(this);
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(fields);
        out.writeObject(options);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        name = in.readString();
        fields = in.readObject();
        options = in.readObject();
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TYPE;
    }

    public static class TypeField implements IdentifiedDataSerializable, Serializable {
        private String name;
        private QueryDataType queryDataType;

        public TypeField() { }

        public TypeField(final String name, final QueryDataType queryDataType) {
            this.name = name;
            this.queryDataType = queryDataType;
        }

        public String getName() {
            return name;
        }

        public QueryDataType getQueryDataType() {
            return queryDataType;
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
