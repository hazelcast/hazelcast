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

package com.hazelcast.sql.impl.partitioner;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Hash function which uses fields to get the hash.
 */
// TODO: Add partitioner for collocated joins that relies on DeclarativePartitioningStrategy.
public class FieldsRowPartitioner extends AbstractFieldsRowPartitioner {
    /** Fields. */
    private List<Integer> fields;

    public FieldsRowPartitioner() {
        // No-op.
    }

    public FieldsRowPartitioner(List<Integer> fields) {
        this.fields = fields;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    protected int getHash(Row row, InternalSerializationService serializationService) {
        int res = 0;

        for (Integer field : fields) {
            int hash = getFieldHash(row.get(field), serializationService);

            res = 31 * res + hash;
        }

        return res;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(fields, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        fields = SerializationUtil.readList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldsRowPartitioner that = (FieldsRowPartitioner) o;

        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{fields=" + fields + '}';
    }
}
