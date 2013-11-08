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
import com.hazelcast.map.record.CachedDataRecord;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class QueryOperation extends AbstractMapOperation {
    Predicate predicate;
    QueryResult result;

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
    }

    public QueryOperation() {
    }

    @Override
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
            runParallel(initialPartitions);
        }
        List<Integer> finalPartitions = mapService.getOwnedPartitions();
        if (initialPartitions.equals(finalPartitions)) {
            result.setPartitionIds(finalPartitions);
        }
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            ((MapService) getService()).getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    private void runParallel(final List<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final SerializationService ss = getNodeEngine().getSerializationService();
        final ExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        final List<Future<ConcurrentMap<Object, QueryableEntry>>> lsFutures = new ArrayList<Future<ConcurrentMap<Object, QueryableEntry>>>(initialPartitions.size());
        for (final Integer partition : initialPartitions) {
            Future<ConcurrentMap<Object, QueryableEntry>> f = executor.submit(new Callable<ConcurrentMap<Object, QueryableEntry>>() {
                public ConcurrentMap<Object, QueryableEntry> call() {
                    final PartitionContainer container = mapService.getPartitionContainer(partition);
                    final RecordStore recordStore = container.getRecordStore(name);
                    ConcurrentMap<Object, QueryableEntry> partitionResult = null;
                    for (Record record : recordStore.getReadonlyRecordMap().values()) {
                        Data key = record.getKey();
                        Object value;
                        if (record instanceof CachedDataRecord) {
                            CachedDataRecord cachedDataRecord = (CachedDataRecord) record;
                            value = cachedDataRecord.getCachedValue();
                            if (value == null) {
                                value = ss.toObject(cachedDataRecord.getValue());
                                cachedDataRecord.setCachedValue(value);
                            }
                        } else {
                            value = record.getValue();
                            if (value instanceof Data) {
                                value = ss.toObject((Data) value);
                            }
                        }
                        if (value == null) {
                            continue;
                        }
                        final QueryEntry queryEntry = new QueryEntry(ss, key, key, value);
                        if (predicate.apply(queryEntry)) {
                            if (partitionResult == null) {
                                partitionResult = new ConcurrentHashMap<Object, QueryableEntry>();
                            }
                            partitionResult.put(queryEntry.getIndexKey(), queryEntry);
                        }
                    }
                    return partitionResult;
                }
            });
            lsFutures.add(f);
        }
        for (Future<ConcurrentMap<Object, QueryableEntry>> future : lsFutures) {
            final ConcurrentMap<Object, QueryableEntry> r = future.get();
            if (r != null) {
                for (QueryableEntry entry : r.values()) {
                    result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
                }
            }
        }
    }

    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        predicate = in.readObject();
    }
}
