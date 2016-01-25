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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Contains the result of a query evaluation.
 *
 * A QueryResults is a collections of {@link QueryResultRow} instances.
 */
public class QueryResult implements IdentifiedDataSerializable, Iterable<QueryResultRow> {

    // todo: probably arraylist cheaper.
    private final Collection<QueryResultRow> rows = new LinkedList<QueryResultRow>();

    private Collection<Integer> partitionIds;

    private transient long resultLimit;
    private transient long resultSize;
    private IterationType iterationType;

    public QueryResult() {
        resultLimit = Long.MAX_VALUE;
    }

    public QueryResult(IterationType iterationType, long resultLimit) {
        this.resultLimit = resultLimit;
        this.iterationType = iterationType;
    }

    // for testing
    IterationType getIterationType() {
        return iterationType;
    }

    @Override
    public Iterator<QueryResultRow> iterator() {
        return rows.iterator();
    }

    public int size() {
        return rows.size();
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    // just for testing
    long getResultLimit() {
        return resultLimit;
    }

    public void addAllRows(Collection<QueryResultRow> r) {
        rows.addAll(r);
    }

    public void addRow(QueryResultRow row) {
        rows.add(row);
    }

    public void addAll(Collection<QueryableEntry> entries) {
        for (QueryableEntry entry : entries) {
            if (++resultSize > resultLimit) {
                throw new QueryResultSizeExceededException();
            }

            Data key = null;
            Data value = null;
            switch (iterationType) {
                case KEY:
                    key = entry.getKeyData();
                    break;
                case VALUE:
                    value = entry.getValueData();
                    break;
                case ENTRY:
                    key = entry.getKeyData();
                    value = entry.getValueData();
                    break;
                default:
                    throw new IllegalStateException("Unknown iterationtype:" + iterationType);
            }

            rows.add(new QueryResultRow(key, value));
        }
    }

    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public Collection<QueryResultRow> getRows() {
        return rows;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int partitionSize = (partitionIds == null) ? 0 : partitionIds.size();
        out.writeInt(partitionSize);
        if (partitionSize > 0) {
            for (Integer partitionId : partitionIds) {
                out.writeInt(partitionId);
            }
        }

        out.writeByte(iterationType.getId());

        int resultSize = rows.size();
        out.writeInt(resultSize);
        if (resultSize > 0) {
            for (QueryResultRow row : rows) {
                row.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int partitionSize = in.readInt();
        if (partitionSize > 0) {
            partitionIds = new ArrayList<Integer>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partitionIds.add(in.readInt());
            }
        }

        iterationType = IterationType.getById(in.readByte());

        int resultSize = in.readInt();
        if (resultSize > 0) {
            for (int i = 0; i < resultSize; i++) {
                QueryResultRow row = new QueryResultRow();
                row.readData(in);
                rows.add(row);
            }
        }
    }
}
