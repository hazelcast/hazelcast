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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link Searchable} expression.
 */
public class SearchableExpression<C extends Comparable<C>> implements Expression<Searchable<C>>, IdentifiedDataSerializable {

    private QueryDataType type;
    private Searchable<C> searchable;

    public SearchableExpression() {
    }

    private SearchableExpression(QueryDataType type, Searchable<C> searchable) {
        this.type = type;
        this.searchable = searchable;
    }

    public static SearchableExpression<?> create(QueryDataType type, Searchable<?> searchable) {
        return new SearchableExpression<>(type, searchable);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_SEARCHABLE;
    }

    @Override
    public Searchable<C> eval(Row row, ExpressionEvalContext context) {
        return searchable;
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeObject(searchable);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = in.readObject();
        searchable = in.readObject();
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (searchable != null ? searchable.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SearchableExpression<?> that = (SearchableExpression<?>) o;

        return Objects.equals(type, that.type) && Objects.equals(searchable, that.searchable);
    }

    @Override
    public String toString() {
        return "SearchableExpression{type=" + type + ", searchable=" + searchable + '}';
    }
}
