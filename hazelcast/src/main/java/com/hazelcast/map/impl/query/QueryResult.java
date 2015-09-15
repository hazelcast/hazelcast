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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class QueryResult implements DataSerializable {

    private final Collection<QueryResultEntry> result = new LinkedList<QueryResultEntry>();
    private IterationType iterationType;
    private Collection<Integer> partitionIds;

    private transient long resultLimit;
    private transient long resultSize;

    public QueryResult() {
        this.resultLimit = Long.MAX_VALUE;
    }

    public QueryResult(IterationType iterationType, long resultLimit) {
        this.resultLimit = resultLimit;
        this.iterationType = iterationType;
    }

    public void addAll(Collection<QueryableEntry> queryableEntries) {
        for (QueryableEntry entry : queryableEntries) {
            if (++resultSize > resultLimit) {
                throw new QueryResultSizeExceededException();
            }
            QueryResultEntryImpl queryEntry = new QueryResultEntryImpl(
                    entry.getKeyData(), entry.getIndexKey(), entry.getValueData());
            result.add(queryEntry);
        }
    }

    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public Collection<QueryResultEntry> getResult() {
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByte(iterationType.getId());

        // writing partition id's
        int partitionSize = (partitionIds == null) ? 0 : partitionIds.size();
        out.writeInt(partitionSize);
        if (partitionSize > 0) {
            for (Integer partitionId : partitionIds) {
                out.writeInt(partitionId);
            }
        }

        // writing results
        int resultSize = result.size();
        out.writeInt(resultSize);
        Iterator<QueryResultEntry> iterator = result.iterator();
        for (int i = 0; i < resultSize; i++) {
            QueryResultEntryImpl queryableEntry = (QueryResultEntryImpl) iterator.next();
            switch (iterationType) {
                case ENTRY:
                    out.writeData(queryableEntry.getKeyData());
                    out.writeData(queryableEntry.getValueData());
                    break;
                case KEY:
                    out.writeData(queryableEntry.getKeyData());
                    break;
                case VALUE:
                    out.writeData(queryableEntry.getKeyData());
                    out.writeData(queryableEntry.getValueData());
                    break;
                default:
                    throw new IllegalStateException("Unhandled " + iterationType);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        iterationType = IterationType.getById(in.readByte());

        // writing partition id's
        int partitionSize = in.readInt();
        if (partitionSize > 0) {
            partitionIds = new ArrayList<Integer>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partitionIds.add(in.readInt());
            }
        }

        // writing the content
        int resultSize = in.readInt();
        for (int i = 0; i < resultSize; i++) {
            Data key = null;
            Data value = null;

            switch (iterationType) {
                case ENTRY:
                    key = in.readData();
                    value = in.readData();
                    break;
                case KEY:
                    key = in.readData();
                    break;
                case VALUE:
                    key = in.readData();
                    value = in.readData();
                    break;
                default:
                    throw new IllegalStateException("Unhandled " + iterationType);
            }

            result.add(new QueryResultEntryImpl(key, null, value));
        }
    }
}
