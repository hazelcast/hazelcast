/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.QueryResult;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.util.SortingUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class QueryOperation extends AbstractMapOperation {

    Predicate predicate;
    QueryResult result;
    PagingPredicate pagingPredicate;

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
        }
    }

    public QueryOperation() {
    }

    public void run() throws Exception {
        List<Integer> initialPartitions = mapService.getOwnedPartitions();
        IndexService indexService = mapService.getMapContainer(name).getIndexService();
        Set<QueryableEntry> entries = null;
        // TODO: fix
        if (!getNodeEngine().getPartitionService().hasOnGoingMigration()) {
            entries = indexService.query(predicate);
        }
        result = new QueryResult();
        if (entries != null) {
            for (QueryableEntry entry : entries) {
                result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
            }
        } else {
            // run in parallel
            if (pagingPredicate != null) {
                runParallelForPaging(initialPartitions);
            } else {
                runParallel(initialPartitions);
            }
        }
        List<Integer> finalPartitions = mapService.getOwnedPartitions();
        if (initialPartitions.equals(finalPartitions)) {
            result.setPartitionIds(finalPartitions);
        }
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            ((MapService) getService()).getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    protected void runParallel(final List<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final SerializationService ss = getNodeEngine().getSerializationService();
        final ExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        final List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(initialPartitions.size());
        for (final Integer partition : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(ss, partition, null));
            lsFutures.add(f);
        }
        for (Future<Collection<QueryableEntry>> future : lsFutures) {
            final Collection<QueryableEntry> collection = future.get();
            if (collection != null) {
                for (QueryableEntry entry : collection) {
                    result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
                }
            }
        }
    }

    protected void runParallelForPaging(List<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final SerializationService ss = getNodeEngine().getSerializationService();
        final ExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        final List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(initialPartitions.size());

        final Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        for (final Integer partition : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(ss, partition, wrapperComparator));
            lsFutures.add(f);
        }
        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        for (Future<Collection<QueryableEntry>> future : lsFutures) {
            final Collection<QueryableEntry> collection = future.get();
            toMerge.addAll(collection);
        }
        Collections.sort(toMerge, wrapperComparator);
        if (toMerge.size() > pagingPredicate.getPageSize()) {
            toMerge = toMerge.subList(0, pagingPredicate.getPageSize());
        }
        for (QueryableEntry entry : toMerge) {
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
        }
    }

    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    public Object getResponse() {
        return result;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeObject(predicate);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        predicate = in.readObject();
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
        }
    }

    private class PartitionCallable implements Callable<Collection<QueryableEntry>> {

        int partition;
        SerializationService ss;
        Comparator<Map.Entry> wrapperComparator;


        private PartitionCallable(SerializationService ss, int partition, Comparator<Map.Entry> wrapperComparator) {
            this.ss = ss;
            this.partition = partition;
            this.wrapperComparator = wrapperComparator;
        }

        public Collection<QueryableEntry> call() throws Exception {
            final PartitionContainer container = mapService.getPartitionContainer(partition);
            final RecordStore recordStore = container.getRecordStore(name);
            LinkedList<QueryableEntry> partitionResult = new LinkedList<QueryableEntry>();
            for (Record record : recordStore.getReadonlyRecordMapByWaitingMapStoreLoad().values()) {
                final Data key = record.getKey();
                Object value = record.getCachedValue();
                if (value == Record.NOT_CACHED) {
                    value = record.getValue();
                    if (value != null && value instanceof Data) {
                        value = ss.toObject(value);
                    }
                } else {
                    value = ss.toObject(record.getValue());
                    record.setCachedValue(value);
                }
                if (value == null) {
                    continue;
                }
                final QueryEntry queryEntry = new QueryEntry(ss, key, key, value);
                if (predicate.apply(queryEntry)) {
                    if (pagingPredicate != null) {
                        Map.Entry anchor = pagingPredicate.getAnchor();
                        final Comparator comparator = pagingPredicate.getComparator();
                        if (anchor != null &&
                                SortingUtil.compare(comparator, pagingPredicate.getIterationType(), anchor, queryEntry) >= 0) {
                            continue;
                        }
                    }
                    partitionResult.add(queryEntry);
                }
            }
            if (pagingPredicate != null) {
                Collections.sort(partitionResult, wrapperComparator);
                if (partitionResult.size() > pagingPredicate.getPageSize()) {
                    return partitionResult.subList(0, pagingPredicate.getPageSize());
                }
            }
            return partitionResult;
        }
    }

}
