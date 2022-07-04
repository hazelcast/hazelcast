/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.SortingUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullablePartitionIdSet;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullablePartitionIdSet;

/**
 * Represents a result of a query execution in the form of an iterable
 * collection of {@link QueryResultRow rows}.
 * <p>
 * There are two modes of result construction, the choice is
 * controlled by the {@code orderAndLimitExpected} parameter:
 *
 * <ol>
 *     <li>When {@code orderAndLimitExpected} is {@code true}, this indicates that
 *     the call to the {@link #orderAndLimit} method is expected on behalf of the
 *     {@link PagingPredicate paging predicate} involved in the query. In this case,
 *     the intermediate result is represented as a collection of {@link
 *     QueryableEntry queryable entries} to allow the comparison of the items using
 *     the comparator of the paging predicate. After the call to {@link
 *     #completeConstruction}, all the queryable entries are converted to {@link
 *     QueryResultRow rows} and the result is ready to be provided to the client.
 *
 *     <li>When {@code orderAndLimitExpected} is {@code false}, this indicates that
 *     no calls to the {@link #orderAndLimit} method are expected. In this case, the
 *     intermediate result is represented directly as a collection of {@link
 *     QueryResultRow rows} and no further conversion is performed.
 * </ol>
 */
public class QueryResult implements Result<QueryResult>, Iterable<QueryResultRow> {

    private List rows = new ArrayList();

    private PartitionIdSet partitionIds;
    private IterationType iterationType;

    private final transient SerializationService serializationService;
    private final transient long resultLimit;
    private final transient boolean orderAndLimitExpected;
    private final transient Projection projection;

    private transient long resultSize;

    /**
     * Constructs an empty result for the purposes of deserialization.
     */
    public QueryResult() {
        serializationService = null;
        orderAndLimitExpected = false;
        resultLimit = Long.MAX_VALUE;
        projection = null;
    }

    /**
     * Constructs an empty result.
     *
     * @param iterationType         the iteration type of the query for which
     *                              this result is constructed for.
     * @param projection            the projection of the query for which this
     *                              result is constructed for.
     * @param serializationService  the serialization service associated with
     *                              the query for which this result is
     *                              constructed for.
     * @param resultLimit           the upper limit on the number of items that
     *                              can be {@link #add added} to this result.
     * @param orderAndLimitExpected the flag to signal that the call to the
     *                              {@link #orderAndLimit} method is expected,
     *                              see the class javadoc for more details.
     */
    public QueryResult(IterationType iterationType, Projection projection, SerializationService serializationService,
                       long resultLimit, boolean orderAndLimitExpected) {
        this.iterationType = iterationType;
        this.projection = projection;
        this.serializationService = serializationService;
        this.resultLimit = resultLimit;
        this.orderAndLimitExpected = orderAndLimitExpected;
    }

    // for testing
    IterationType getIterationType() {
        return iterationType;
    }

    @Override
    public Iterator<QueryResultRow> iterator() {
        return rows.iterator();
    }

    /**
     * @return the size of this result.
     */
    public int size() {
        return rows.size();
    }

    /**
     * @return {@code true} if this result is empty, {@code false} otherwise.
     **/
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /**
     * Adds the given row into this result.
     *
     * @param row the row to add.
     */
    public void addRow(QueryResultRow row) {
        rows.add(row);
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueryResultSizeExceededException if the size of this result
     *                                          exceeds the result size limit.
     */
    @Override
    public void add(QueryableEntry entry) {
        if (++resultSize > resultLimit) {
            throw new QueryResultSizeExceededException();
        }

        rows.add(orderAndLimitExpected ? entry : convertEntryToRow(entry));
    }

    @Override
    public QueryResult createSubResult() {
        return new QueryResult(iterationType, projection, serializationService, resultLimit, orderAndLimitExpected);
    }

    @Override
    public void orderAndLimit(PagingPredicate pagingPredicate, Map.Entry<Integer, Map.Entry> nearestAnchorEntry) {
        rows = SortingUtil.getSortedSubList(rows, pagingPredicate, nearestAnchorEntry);
    }

    @Override
    public void completeConstruction(PartitionIdSet partitionIds) {
        setPartitionIds(partitionIds);
        if (orderAndLimitExpected) {
            for (ListIterator iterator = rows.listIterator(); iterator.hasNext(); ) {
                iterator.set(convertEntryToRow((QueryableEntry) iterator.next()));
            }
        }
    }

    private Data getValueData(QueryableEntry entry) {
        if (projection != null) {
            return serializationService.toData(projection.transform(entry));
        } else {
            return entry.getValueData();
        }
    }

    @Override
    public PartitionIdSet getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void combine(QueryResult result) {
        PartitionIdSet otherPartitionIds = result.getPartitionIds();
        if (otherPartitionIds == null) {
            return;
        }
        if (partitionIds == null) {
            partitionIds = new PartitionIdSet(otherPartitionIds);
        } else {
            partitionIds.addAll(otherPartitionIds);
        }
        rows.addAll(result.rows);
    }

    @Override
    public void onCombineFinished() {
    }

    @Override
    public void setPartitionIds(PartitionIdSet partitionIds) {
        this.partitionIds = new PartitionIdSet(partitionIds);
    }

    /**
     * @return the rows of this result.
     */
    public List<QueryResultRow> getRows() {
        return rows;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.QUERY_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeNullablePartitionIdSet(partitionIds, out);
        out.writeByte(iterationType.getId());

        int resultSize = rows.size();
        out.writeInt(resultSize);
        if (resultSize > 0) {
            for (QueryResultRow row : (List<QueryResultRow>) rows) {
                row.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionIds = readNullablePartitionIdSet(in);

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

    private QueryResultRow convertEntryToRow(QueryableEntry entry) {
        Data key = null;
        Data value = null;
        switch (iterationType) {
            case KEY:
                key = entry.getKeyData();
                break;
            case VALUE:
                value = getValueData(entry);
                break;
            case ENTRY:
                key = entry.getKeyData();
                value = entry.getValueData();
                break;
            default:
                throw new IllegalStateException("Unknown iterationType:" + iterationType);
        }
        return new QueryResultRow(key, value);
    }
}
