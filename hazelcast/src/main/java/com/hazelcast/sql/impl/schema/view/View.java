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

package com.hazelcast.sql.impl.schema.view;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class View implements IdentifiedDataSerializable {

    private String name;
    private String query;
    private List<String> projection;

    public View() {
    }

    public View(String name, String query, List<String> projection) {
        this.name = name;
        this.query = query;
        this.projection = projection;
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    public List<String> projection() {
        return projection;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
        SerializationUtil.writeList(projection, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        query = in.readString();
        projection = SerializationUtil.readList(in);
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
                && Objects.equals(projection, view.projection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query);
    }
}
