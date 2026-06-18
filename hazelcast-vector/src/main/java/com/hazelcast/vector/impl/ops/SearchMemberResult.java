/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;

import java.io.IOException;
import java.util.Objects;

public final class SearchMemberResult implements IdentifiedDataSerializable {
    private SearchResults<Data, Data> results;
    private PartitionIdSet scannedPartitions;

    public SearchMemberResult() {
    }

    public SearchMemberResult(SearchResults<Data, Data> results, PartitionIdSet scannedPartitions) {
        this.results = results;
        this.scannedPartitions = scannedPartitions;
    }

    public SearchResults<Data, Data> results() {
        return results;
    }

    public PartitionIdSet scannedPartitions() {
        return scannedPartitions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (SearchMemberResult) obj;
        return Objects.equals(this.results, that.results)
                && Objects.equals(this.scannedPartitions, that.scannedPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, scannedPartitions);
    }

    @Override
    public String toString() {
        return "SearchMemberResult["
                + "results=" + results + ", "
                + "scannedPartitions=" + scannedPartitions + ']';
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.SEARCH_MEMBER_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(results);
        out.writeObject(scannedPartitions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        results = in.readObject();
        scannedPartitions = in.readObject();
    }
}
