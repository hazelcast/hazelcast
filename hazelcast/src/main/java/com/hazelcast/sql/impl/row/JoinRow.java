/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Objects;

/**
 * A row which joins two other rows.
 */
public class JoinRow implements Row, IdentifiedDataSerializable {
    /** Left row. */
    private Row left;

    /** Right row. */
    private Row right;

    public JoinRow() {
        // No-op.
    }

    public JoinRow(Row left, Row right) {
        this.left = left;
        this.right = right;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int idx) {
        int leftColumnCount = left.getColumnCount();

        if (idx < leftColumnCount) {
            return (T) left.get(idx);
        } else {
            return (T) right.get(idx - leftColumnCount);
        }
    }

    @Override
    public int getColumnCount() {
        return left.getColumnCount() + right.getColumnCount();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.ROW_JOIN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JoinRow joinRow = (JoinRow) o;

        return left.equals(joinRow.left) && right.equals(joinRow.right);
    }
}
