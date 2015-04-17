/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;

public class QueryResult implements DataSerializable {

    private final Collection<QueryResultEntry> result;

    private boolean isResultLimitExceeded;
    private Collection<Integer> partitionIds;

    public QueryResult() {
        result = new LinkedHashSet<QueryResultEntry>();
    }

    @SuppressWarnings("unused")
    public QueryResult(Collection<QueryResultEntry> queryableEntries) {
        result = queryableEntries;
    }

    public boolean isResultLimitExceeded() {
        return isResultLimitExceeded;
    }

    public void setResultLimitExceeded() {
        isResultLimitExceeded = true;
    }

    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public void add(QueryResultEntry resultEntry) {
        result.add(resultEntry);
    }

    public Collection<QueryResultEntry> getResult() {
        return result;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(isResultLimitExceeded);
        int partitionSize = (partitionIds == null) ? 0 : partitionIds.size();
        out.writeInt(partitionSize);
        if (partitionSize > 0) {
            for (Integer partitionId : partitionIds) {
                out.writeInt(partitionId);
            }
        }
        int resultSize = result.size();
        out.writeInt(resultSize);
        if (resultSize > 0) {
            Iterator<QueryResultEntry> iterator = result.iterator();
            for (int i = 0; i < resultSize; i++) {
                QueryResultEntryImpl queryableEntry = (QueryResultEntryImpl) iterator.next();
                queryableEntry.writeData(out);
            }
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        isResultLimitExceeded = in.readBoolean();
        int partitionSize = in.readInt();
        if (partitionSize > 0) {
            partitionIds = new ArrayList<Integer>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partitionIds.add(in.readInt());
            }
        }
        int resultSize = in.readInt();
        if (resultSize > 0) {
            for (int i = 0; i < resultSize; i++) {
                QueryResultEntryImpl resultEntry = new QueryResultEntryImpl();
                resultEntry.readData(in);
                result.add(resultEntry);
            }
        }
    }
}
