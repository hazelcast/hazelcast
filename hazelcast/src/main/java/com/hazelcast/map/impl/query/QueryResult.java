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
import java.util.List;

import static java.lang.System.arraycopy;

/**
 * Contains the result of a query evaluation.
 *
 * A QueryResults is a collections of {@link QueryResultRow} instances.
 *
 * The QueryResult is an append only data-structure.
 */
public class QueryResult implements IdentifiedDataSerializable {

    private static final int DEFAULT_CAPACITY = 256;
    private static final int GROW_FACTOR = 4;

    private int size;
    private Data[] keys;
    private Data[] values;

    private Collection<Integer> partitionIds;

    private transient long resultLimit;
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

    public Cursor openCursor() {
        return new Cursor();
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    // just for testing
    long getResultLimit() {
        return resultLimit;
    }

    public void add(Data key, Data value) {
        ensureCapacity(size + 1);
        addInternal(key, value);
    }

    public void addFrom(QueryResultRow row) {
        ensureCapacity(size + 1);
        addInternal(row.getKey(), row.getValue());
    }

    public void addFrom(QueryableEntry entry) {
        ensureCapacity(size + 1);
        switch (iterationType) {
            case KEY:
                addInternal(entry.getKeyData(), null);
                break;
            case VALUE:
                addInternal(null, entry.getValueData());
                break;
            case ENTRY:
                addInternal(entry.getKeyData(), entry.getValueData());
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
    }

    public void addAllFrom(QueryResult result) {
        if (result.iterationType != iterationType) {
            throw new IllegalArgumentException("IterationType mismatch, expected:" + iterationType
                    + " but found:" + result.iterationType);
        }

        if (result.isEmpty()) {
            return;
        }

        ensureCapacity(size + result.size);

        switch (iterationType) {
            case KEY:
                arraycopy(result.keys, 0, keys, size, result.size);
                break;
            case VALUE:
                arraycopy(result.values, 0, values, size, result.size);
                break;
            case ENTRY:
                arraycopy(result.keys, 0, keys, size, result.size);
                arraycopy(result.values, 0, values, size, result.size);
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
        size += result.size;
    }

    public void addAllFrom(Collection<QueryableEntry> entries) {
        ensureCapacity(size + entries.size());

        switch (iterationType) {
            case KEY:
                for (QueryableEntry entry : entries) {
                    addInternal(entry.getKeyData(), null);
                }
                break;
            case VALUE:
                for (QueryableEntry entry : entries) {
                    addInternal(null, entry.getValueData());
                }
                break;
            case ENTRY:
                for (QueryableEntry entry : entries) {
                    addInternal(entry.getKeyData(), entry.getValueData());
                }
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > resultLimit) {
            throw new QueryResultSizeExceededException();
        }

        int currentCapacity = 0;
        if (keys != null) {
            currentCapacity = keys.length;
        } else if (values != null) {
            currentCapacity = values.length;
        }

        if (minCapacity <= currentCapacity) {
            return;
        }

        switch (iterationType) {
            case KEY:
                keys = grow(keys, minCapacity);
                break;
            case VALUE:
                values = grow(values, minCapacity);
                break;
            case ENTRY:
                keys = grow(keys, minCapacity);
                values = grow(values, minCapacity);
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
    }

    private Data[] grow(Data[] array, int minimumCapacity) {
        int newCapacity = array == null ? DEFAULT_CAPACITY : GROW_FACTOR * array.length;

        while (newCapacity < minimumCapacity) {
            newCapacity *= 2;
        }

        if (newCapacity > resultLimit) {
            newCapacity = (int) resultLimit;
        }

        if (array == null) {
            return new Data[newCapacity];
        }

        Data[] newArray = new Data[newCapacity];
        arraycopy(array, 0, newArray, 0, array.length);
        return newArray;
    }

    /**
     * Adds an item.
     *
     * This method relies on the fact that the capacity has been ensured.
     */
    private void addInternal(Data key, Data value) {
        switch (iterationType) {
            case KEY:
                keys[size] = key;
                break;
            case VALUE:
                values[size] = value;
                break;
            case ENTRY:
                keys[size] = key;
                values[size] = value;
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
        size++;
    }

    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    // todo: this method causes huge amount of litter.
    public List<QueryResultRow> getRows() {
        List<QueryResultRow> result = new ArrayList<QueryResultRow>(size);
        for (Cursor cursor = openCursor(); cursor.next(); ) {
            result.add(new QueryResultRow(cursor.getKey(), cursor.getValue()));
        }
        return result;
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
    public void readData(ObjectDataInput in) throws IOException {
        int partitionSize = in.readInt();
        if (partitionSize > 0) {
            partitionIds = new ArrayList<Integer>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                partitionIds.add(in.readInt());
            }
        }

        iterationType = IterationType.getById(in.readByte());

        size = in.readInt();
        switch (iterationType) {
            case KEY:
                keys = new Data[size];
                for (int k = 0; k < size; k++) {
                    keys[k] = in.readData();
                }
                break;
            case VALUE:
                values = new Data[size];
                for (int k = 0; k < size; k++) {
                    values[k] = in.readData();
                }
                break;
            case ENTRY:
                keys = new Data[size];
                values = new Data[size];
                for (int k = 0; k < size; k++) {
                    keys[k] = in.readData();
                    values[k] = in.readData();
                }
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
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

        out.writeInt(size);
        switch (iterationType) {
            case KEY:
                for (int k = 0; k < size; k++) {
                    out.writeData(keys[k]);
                }
                break;
            case VALUE:
                for (int k = 0; k < size; k++) {
                    out.writeData(values[k]);
                }
                break;
            case ENTRY:
                for (int k = 0; k < size; k++) {
                    out.writeData(keys[k]);
                    out.writeData(values[k]);
                }
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
    }

    public class Cursor {
        private int index = -1;

        /**
         * Gets the key.
         *
         * @return the key, or null if no key is available.
         */
        public Data getKey() {
            return keys == null ? null : keys[index];
        }

        /**
         * Gets the value.
         *
         * @return the value, or null of no value is available.
         */
        public Data getValue() {
            return values == null ? null : values[index];
        }

        /**
         * Checks if there is a next item.
         *
         * @return true if there is a next item, false otherwise.
         */
        public boolean hasNext() {
            return index + 1 < size;
        }

        /**
         * Positions the cursor on the next item.
         *
         * @return true if a next item was found, false otherwise.
         */
        public boolean next() {
            if (index == size - 1) {
                return false;
            }

            index++;
            return true;
        }
    }
}
