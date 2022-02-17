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

package com.hazelcast.sql.impl.row;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A row object that's sent between processors in the Jet SQL engine. It
 * contains a fixed number of values. The values can be either in
 * serialized or deserialized form, the {@link #get} method returns a
 * deserialized value and deserializes it if needed, the {@link
 * #getSerialized} method returns a serialized value and serializes it
 * if needed. The {@link #getMaybeSerialized} method might return
 * serialized or deserialized value, depending on the current status.
 * <p>
 * The class is not thread-safe. Therefore, processors returning it must
 * not modify it after returning it, otherwise races are possible.
 */
public class JetSqlRow implements IdentifiedDataSerializable {

    private SerializationService ss;
    private Object[] values;

    // for deserialization
    public JetSqlRow() { }

    public JetSqlRow(@Nonnull SerializationService ss, @Nonnull Object[] values) {
        this.ss = ss;
        this.values = values;
    }

    public Object get(int index) {
        values[index] = ss.toObject(values[index]);
        return values[index];
    }

    public Data getSerialized(int index) {
        values[index] = ss.toData(values[index]);
        return (Data) values[index];
    }

    public Object getMaybeSerialized(int index) {
        return values[index];
    }

    public int getFieldCount() {
        return values.length;
    }

    public Object[] getValues() {
        return values;
    }

    public SerializationService getSerializationService() {
        return ss;
    }

    public Row getRow() {
        return new Row() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int index) {
                return (T) JetSqlRow.this.get(index);
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
        return extendBy == 0 ? this : new JetSqlRow(ss, Arrays.copyOf(values, values.length + extendBy));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JetSqlRow jetSqlRow = (JetSqlRow) o;
        for (int i = 0; i < values.length; i++) {
            // we compare the serialized form
            if (!Objects.equals(getSerialized(i), jetSqlRow.getSerialized(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // This is a dummy value that will not break the contract, but the object is not supposed
        // to be used as a hash map key.
        return 0;
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.JET_SQL_ROW;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.length);
        for (Object value : values) {
            IOUtil.writeData(out, ss.toData(value));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ss = ((SerializationServiceSupport) in).getSerializationService();
        values = new Object[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = IOUtil.readData(in);
        }
    }
}
