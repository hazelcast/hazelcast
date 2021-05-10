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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.SqlSerializationHooks;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.Arrays;

public class JetSqlRow implements IdentifiedDataSerializable {

    private Object[] values;

    // for deserialization
    public JetSqlRow() { }

    public JetSqlRow(int fieldCount) {
        values = new Object[fieldCount];
    }

    public JetSqlRow(Object[] values) {
        this.values = values;
    }

    public Object get(InternalSerializationService ss, int index) {
        if (values[index] instanceof Data) {
            values[index] = ss.toObject(values[index]);
        }
        return values[index];
    }

    public Data getSerialized(InternalSerializationService ss, int index) {
        if (!(values[index] instanceof Data)) {
            values[index] = ss.toData(values[index]);
        }
        return (Data) values[index];
    }

    public Object getMaybeSerialized(int index) {
        return values[index];
    }

    public void set(int index, Object value) {
        values[index] = value;
    }

    public int getFieldCount() {
        return values.length;
    }

    public Object[] getValues() {
        return values;
    }

    public Row getRow(InternalSerializationService ss) {
        return new Row() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int index) {
                return (T) JetSqlRow.this.get(ss, index);
            }

            @Override
            public int getColumnCount() {
                return JetSqlRow.this.getFieldCount();
            }
        };
    }

    /**
     * Creates a copy of this row with the number of fields increased by {@code
     * extendBy}. The added fields will contain {@code null}s. If {@code
     * extendBy == 0}, this instance is returned.
     */
    public JetSqlRow extendedRow(int extendBy) {
        assert extendBy > -1;
        return extendBy == 0 ? this : new JetSqlRow(Arrays.copyOf(values, values.length + extendBy));
    }

    @Override
    public int getFactoryId() {
        return SqlSerializationHooks.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return SqlSerializationHooks.JET_SQL_ROW;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.length);
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        values = new Object[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            // TODO [viliam] always read as Data somehow
            values[i] = in.readObject();
        }
    }
}
