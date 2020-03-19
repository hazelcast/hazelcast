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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MockPhysicalNode implements PhysicalNode {

    private int id;
    private List<QueryDataType> schema;

    public static MockPhysicalNode create(int id, QueryDataType... types) {
        List<QueryDataType> types0 = types == null || types.length == 0 ? Collections.emptyList() : Arrays.asList(types);

        return new MockPhysicalNode(id, types0);
    }

    public MockPhysicalNode() {
        // No-op.
    }

    public MockPhysicalNode(int id, List<QueryDataType> schema) {
        this.id = id;
        this.schema = schema;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public PhysicalNodeSchema getSchema() {
        return new PhysicalNodeSchema(schema);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(id);
        SerializationUtil.writeList(schema, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readInt();
        schema = SerializationUtil.readList(in);
    }

    @Override
    public int hashCode() {
        int res = id;

        for (QueryDataType type : schema) {
            res = 31 * res + type.hashCode();
        }

        return res;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MockPhysicalNode that = (MockPhysicalNode) o;

        return id == that.id && schema.equals(that.schema);
    }
}
