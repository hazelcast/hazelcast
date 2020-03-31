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

package com.hazelcast.sql.impl.row.hash;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;

/**
 * Hash function which uses all row columns to calculate the hash.
 */
public class AllFieldsRowHashFunction implements RowHashFunction, DataSerializable {
    /** Singleton instance. */
    public static final AllFieldsRowHashFunction INSTANCE = new AllFieldsRowHashFunction();

    public AllFieldsRowHashFunction() {
        // No-op.
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public int getHash(Row row) {
        int res = 0;

        for (int idx = 0; idx < row.getColumnCount(); idx++) {
            Object val = row.get(idx);
            int hash = val != null ? val.hashCode() : 0;

            res = 31 * res + hash;
        }

        return res;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // No-op.
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AllFieldsRowHashFunction;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{}";
    }
}
