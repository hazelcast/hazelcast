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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;

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
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        return value.getValue(evalContext);
    }

    @Override
    public boolean isCooperative() {
        return value.isCooperative();
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INDEX_FILTER_EQUALS;
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
