/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.sql.impl.schema.view;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Objects;

public class View implements IdentifiedDataSerializable {

    private String name;
    private String query;

    public View() {
    }

    public View(String name, String query) {
        this.name = name;
        this.query = query;
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        query = in.readString();
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
        return Objects.equals(name, view.name) && Objects.equals(query, view.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query);
    }
}
