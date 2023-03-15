/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A value of a ROW type (ROW is a SQL struct). It contains values, but not the
 * field names - names are part of the type, the value doesn't reference the
 * type.
 * This class is likely to change in the future release.
 */
@Beta
public class RowValue implements Serializable, IdentifiedDataSerializable {
    private List<Object> values;

    public RowValue() {
        values = new ArrayList<>();
    }

    public RowValue(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(final List<Object> values) {
        this.values = values;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeInt(values.size());
        for (final Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        final int size = in.readInt();
        this.values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.values.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.ROW_VALUE;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RowValue rowValue = (RowValue) o;
        return Objects.equals(values, rowValue.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return "[" + values + ']';
    }
}
