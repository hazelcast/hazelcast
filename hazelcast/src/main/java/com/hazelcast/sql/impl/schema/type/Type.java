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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Type implements IdentifiedDataSerializable {
    private String name;
    private String javaClassName;
    private List<TypeField> fields;
    private QueryDataType queryDataType;

    public Type() { }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
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

    public QueryDataType getQueryDataType() {
        return queryDataType;
    }

    public void setQueryDataType(final QueryDataType queryDataType) {
        this.queryDataType = queryDataType;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(javaClassName);
        out.writeObject(fields);
        out.writeObject(queryDataType);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        this.name = in.readString();
        this.javaClassName = in.readString();
        this.fields = in.readObject(Map.class);
        this.queryDataType = in.readObject(QueryDataType.class);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TYPE;
    }

    public static class TypeField implements IdentifiedDataSerializable {
        private String name;
        private QueryDataType queryDataType;
        private String className = "";

        public TypeField() { }

        public TypeField(final String name, final QueryDataType queryDataType) {
            this.name = name;
            this.queryDataType = queryDataType;
        }

        public TypeField(final String name, final String className) {
            this.name = name;
            this.className = className;
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

        public String getClassName() {
            return className;
        }

        public void setClassName(final String className) {
            this.className = className;
        }

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeObject(queryDataType);
            out.writeString(className);
        }

        @Override
        public void readData(final ObjectDataInput in) throws IOException {
            this.name = in.readString();
            this.queryDataType = in.readObject(QueryDataType.class);
            this.className = in.readString();
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
