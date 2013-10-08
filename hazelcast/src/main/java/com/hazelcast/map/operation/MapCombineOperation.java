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

import com.hazelcast.map.EntryMapper;
import com.hazelcast.map.EntryReducer;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapReduceOutput;

import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.CachedDataRecord;
import com.hazelcast.map.record.DataRecord;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ExecutionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MapCombineOperation extends AbstractMapOperation {
    EntryMapper mapper;
    EntryReducer combiner;
    transient MapEntrySet response;

    public MapCombineOperation(String name, EntryMapper mapper, EntryReducer combiner) {
        super(name);
        this.mapper = mapper;
        this.combiner = combiner;
    }

    public MapCombineOperation() {
    }

    @Override
    public void run() throws Exception {
        response = new MapEntrySet();
        List<Integer> initialPartitions = mapService.getOwnedPartitions().get();

        runParallel(initialPartitions);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            ((MapService) getService()).getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    private void runParallel(final List<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final SerializationService ss = getNodeEngine().getSerializationService();
        final ExecutorService executor = getNodeEngine().getExecutionService().getExecutor(ExecutionService.MAPREDUCE_EXECUTOR);
        final List<Future<Map<Object,List<Object>>>> lsFutures = new ArrayList<Future<Map<Object,List<Object>>>>(initialPartitions.size());
        for (final Integer partition : initialPartitions) {
            Future<Map<Object,List<Object>>> f = executor.submit(new Callable<Map<Object,List<Object>>>() {
                public Map<Object,List<Object>> call() {
                    final PartitionContainer container = mapService.getPartitionContainer(partition);
                    final RecordStore recordStore = container.getRecordStore(name);
                    final MapReduceOutputImpl output = new MapReduceOutputImpl();
                    ConcurrentMap<Object, Object> partitionResult = null;
                    for (Record record : recordStore.getRecords().values()) {
                        Object key = ss.toObject(record.getKey());
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

                        if (value == null) {
                            continue;
                        }

                        mapper.process(key, value, output);
                    }

                    return output.getStorage();
                }
            });

            lsFutures.add(f);
        }

        Map<Object,List<Object>> temp = new HashMap<Object,List<Object>>();
        for (Future<Map<Object,List<Object>>> future : lsFutures) {
            final Map<Object,List<Object>> r = future.get();
            for (Map.Entry<Object,List<Object>> entry : r.entrySet()) {
                Object key = entry.getKey();
                List<Object> values = entry.getValue();

                List<Object> combined = temp.get(key);
                if (combined == null) {
                    combined = new ArrayList<Object>(values.size());
                    temp.put(key, combined);
                }

                combined.addAll(values);
            }
        }

        if (combiner != null) {
            final MapReduceOutputImpl output = new MapReduceOutputImpl();
            for (Map.Entry<Object,List<Object>> entry : temp.entrySet()) {
                combiner.process(entry.getKey(), entry.getValue(), output);
            }

            temp = output.getStorage();
        }

        for (Map.Entry<Object,List<Object>> entry : temp.entrySet()) {
            response.add(ss.toData(entry.getKey()), ss.toData(entry.getValue()));
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapper = in.readObject();
        combiner = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapper);
        out.writeObject(combiner);
    }

    static final class MapReduceOutputImpl implements MapReduceOutput<Object, Object> {
        private final Map<Object,List<Object>> storage = new HashMap<Object,List<Object>>();

        @Override
        public void write(Object key, Object value) {
            List<Object> perkey = storage.get(key);
            if (perkey == null) {
                perkey = new ArrayList<Object>();
                storage.put(key, perkey);
            }

            perkey.add(value);
        }

        public Map<Object,List<Object>> getStorage() {
            return storage;
        }
    }
}
