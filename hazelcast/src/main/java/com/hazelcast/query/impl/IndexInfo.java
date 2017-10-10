/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class IndexInfo implements IdentifiedDataSerializable, Comparable<IndexInfo> {

    private String attributeName;
    private boolean ordered;

    public IndexInfo() {
    }

    public IndexInfo(String attributeName, boolean ordered) {
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributeName);
        out.writeBoolean(ordered);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attributeName = in.readUTF();
        ordered = in.readBoolean();
    }

    public String getAttributeName() {
        return attributeName;
    }

    public boolean isOrdered() {
        return ordered;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.INDEX_INFO;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexInfo indexInfo = (IndexInfo) o;
        if (ordered != indexInfo.ordered) {
            return false;
        }
        return attributeName != null ? attributeName.equals(indexInfo.attributeName) : indexInfo.attributeName == null;

    }

    @Override
    public int hashCode() {
        int result = attributeName != null ? attributeName.hashCode() : 0;
        result = 31 * result + (ordered ? 1 : 0);
        return result;
    }

    @Override
    public int compareTo(IndexInfo other) {
        int attributeNameCompareResult = attributeName.compareTo(other.attributeName);
        if (attributeNameCompareResult == 0) {
            return Boolean.valueOf(ordered).compareTo(other.ordered);
        }
        return attributeNameCompareResult;
    }
}
