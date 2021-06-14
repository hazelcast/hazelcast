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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.config.IndexType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;

public class IndexSortMetadata implements IdentifiedDataSerializable {
    private IndexType type;
    private boolean descending;
    private String[] attributesName;

    public IndexSortMetadata() {
        // no-op
    }

    public IndexSortMetadata(IndexType type, boolean descending, String[] attributesName) {
        this.type = type;
        this.descending = descending;
        this.attributesName = attributesName;
    }

    public IndexType getType() {
        return type;
    }

    public boolean isDescending() {
        return descending;
    }

    public String[] getAttributeName() {
        return attributesName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(type);
        out.writeBoolean(descending);
        out.writeInt(attributesName.length);
        for (int i = 0; i < attributesName.length; ++i) {
            out.writeString(attributesName[i]);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.type = in.readObject();
        this.descending = in.readBoolean();
        int attributeNameLength = in.readInt();
        this.attributesName = new String[attributeNameLength];
        for (int i = 0; i < attributeNameLength; ++i) {
            this.attributesName[i] = in.readString();
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.INDEX_SORT_METADATA;
    }
}
