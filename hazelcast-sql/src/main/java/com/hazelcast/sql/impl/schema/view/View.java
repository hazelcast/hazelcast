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

package com.hazelcast.sql.impl.schema.view;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.parse.SqlCreateView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class View implements Versioned, SqlCatalogObject {

    private String name;
    private String query;
    private List<String> viewColumnNames;
    private List<QueryDataType> viewColumnTypes;

    public View() {
    }

    public View(String name, String query, List<String> columnNames, List<QueryDataType> columnTypes) {
        this.name = name;
        this.query = query;
        this.viewColumnNames = columnNames;
        this.viewColumnTypes = columnTypes;
    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }

    public List<String> viewColumnNames() {
        return viewColumnNames;
    }

    /**
     * Returns the list of column types for this view.
     * <p>
     * Warning: this information can be stale if objects this view depends on is
     * changed. For example, if this view is defined by `select a from
     * my_mapping` and the type of `a` changes in `my_mapping`, the actual type
     * of the view column will change, but it will not be reflected in the
     * return value of this method, until the view is recreated.
     */
    public List<QueryDataType> viewColumnTypes() {
        return viewColumnTypes;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(query);
        SerializationUtil.writeList(viewColumnNames, out);
        SerializationUtil.writeList(viewColumnTypes, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        query = in.readString();
        viewColumnNames = SerializationUtil.readList(in);
        viewColumnTypes = SerializationUtil.readList(in);
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
                && Objects.equals(viewColumnNames, view.viewColumnNames)
                && Objects.equals(viewColumnTypes, view.viewColumnTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, viewColumnNames, viewColumnTypes);
    }

    @Override
    @Nonnull
    public String unparse() {
        return SqlCreateView.unparse(this);
    }
}
