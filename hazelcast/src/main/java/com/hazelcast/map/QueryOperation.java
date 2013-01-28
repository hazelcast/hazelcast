/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.MultiResultSet;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class QueryOperation extends AbstractNamedOperation {

    MapService mapService;
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
        mapService = getService();
        List<Integer> initialPartitions = mapService.getOwnedPartitions().get();
        IndexService indexService = mapService.getMapContainer(name).getIndexService();
        Set<QueryableEntry> entries = indexService.query(predicate);
        if (entries == null) {
            // run in parallel
            entries = runParallel(initialPartitions);
        }
        List<Integer> finalPartitions = mapService.getOwnedPartitions().get();
        if (initialPartitions == finalPartitions) {
            result = new QueryResult();
            result.setPartitionIds(finalPartitions);
            result.setResult(entries);
        }
    }

    private Set<QueryableEntry> runParallel(final List<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final SerializationServiceImpl ss = (SerializationServiceImpl) getNodeEngine().getSerializationService();
        final ExecutorService executor = getNodeEngine().getExecutionService().getExecutor("parallel-query");
        final List<Future<ConcurrentMap<Object, QueryableEntry>>> lsFutures = new ArrayList<Future<ConcurrentMap<Object, QueryableEntry>>>(initialPartitions.size());
        for (final Integer partition : initialPartitions) {
            Future<ConcurrentMap<Object, QueryableEntry>> f = executor.submit(new Callable<ConcurrentMap<Object, QueryableEntry>>() {
                public ConcurrentMap<Object, QueryableEntry> call() {
                    final PartitionContainer container = mapService.getPartitionContainer(partition);
                    final RecordStore recordStore = container.getRecordStore(name);
                    ConcurrentMap<Object, QueryableEntry> partitionResult = null;
                    for (Record record : recordStore.getRecords().values()) {
                        Object key = record.getKey();
                        Object value = null;
                        if (record instanceof CachedDataRecord) {
                            CachedDataRecord cachedDataRecord = (CachedDataRecord) record;
                            value = cachedDataRecord.getCachedValue();
                            if (value == null) {
                                value = ss.toObject(cachedDataRecord.getValue());
                                cachedDataRecord.setCachedValue(value);
                            }
                        } else if (record instanceof DataRecord) {
                            value = ss.toObject(((DataRecord) record).getValue());
                        } else {
                            value = record.getValue();
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
        MultiResultSet multiResultSet = new MultiResultSet();
        for (Future<ConcurrentMap<Object, QueryableEntry>> future : lsFutures) {
            final ConcurrentMap<Object, QueryableEntry> r = future.get();
            if (r != null) {
                multiResultSet.addResultSet(r);
            }
        }
        return multiResultSet;
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
