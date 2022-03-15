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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Equality filter that is used for "WHERE a = ?" and "WHERE a IS NULL" conditions.
 */
@SuppressWarnings("rawtypes")
public class IndexEqualsFilter implements IndexFilter, IdentifiedDataSerializable {

    private IndexFilterValue value;

    public IndexEqualsFilter() {
        // No-op.
    }

    public IndexEqualsFilter(IndexFilterValue value) {
        this.value = value;
    }

    public IndexFilterValue getValue() {
        return value;
    }

    @Override
    public Iterator<QueryableEntry> getEntries(InternalIndex index, boolean descending, ExpressionEvalContext evalContext) {
        Comparable value = getComparable(evalContext);

        if (value == null) {
            // "WHERE a = NULL" always yields an empty result set.
            return Collections.emptyIterator();
        }

        return index.getSqlRecordIterator(value);
    }

    @Override
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        return value.getValue(evalContext);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.INDEX_FILTER_EQUALS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexEqualsFilter that = (IndexEqualsFilter) o;

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "IndexEqualsFilter {value=" + value + '}';
    }
}
