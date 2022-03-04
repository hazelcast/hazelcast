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

package com.hazelcast.sql.impl.schema.view;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class View implements IdentifiedDataSerializable {

    private String name;
    private String query;
    private boolean isStream;
    private List<String> viewColumnNames;
    private List<QueryDataType> viewColumnTypes;

    public View() {
    }

    public View(String name, String query, boolean isStream, List<String> columnNames, List<QueryDataType> columnTypes) {
        this.name = name;
        this.query = query;
        this.isStream = isStream;
        this.viewColumnNames = columnNames;
        this.viewColumnTypes = columnTypes;
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    public boolean isStream() {
        return isStream;
    }

    public List<String> viewColumnNames() {
        return viewColumnNames;
    }

    public List<QueryDataType> viewColumnTypes() {
        return viewColumnTypes;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
        out.writeBoolean(isStream);
        SerializationUtil.writeList(viewColumnNames, out);
        SerializationUtil.writeList(viewColumnTypes, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        query = in.readString();
        isStream = in.readBoolean();
        viewColumnNames = SerializationUtil.readList(in);
        viewColumnTypes = SerializationUtil.readList(in);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.VIEW;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        View view = (View) o;
        return Objects.equals(name, view.name)
                && Objects.equals(query, view.query)
                && isStream == view.isStream
                && Objects.equals(viewColumnNames, view.viewColumnNames)
                && Objects.equals(viewColumnTypes, view.viewColumnTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, isStream, viewColumnNames, viewColumnTypes);
    }
}
