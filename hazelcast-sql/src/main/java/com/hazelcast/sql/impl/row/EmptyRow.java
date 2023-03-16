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

package com.hazelcast.sql.impl.row;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Row with no columns. It may appear when the downstream operator doesn't need any columns from
 * the upstream, and only the number of returned rows is important.
 * <p>
 * Example:
 * <pre>
 * SELECT GET_DATE() FROM person
 * </pre>
 */
public class EmptyRow implements Row, IdentifiedDataSerializable {

    public static final EmptyRow INSTANCE = new EmptyRow();

    public EmptyRow() {
        // No-op.
    }

    @Override
    public <T> T get(int index) {
        throw new IndexOutOfBoundsException(getClass().getName() + " has no columns");
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.ROW_EMPTY;
    }

    @Override
    public void writeData(ObjectDataOutput out) {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) {
        // No-op.
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EmptyRow;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{}";
    }
}
