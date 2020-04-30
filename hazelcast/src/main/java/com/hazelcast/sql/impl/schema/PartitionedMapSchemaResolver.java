/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class PartitionedMapSchemaResolver extends MapSchemaResolver {
    public PartitionedMapSchemaResolver(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public List<SqlTableSchema> getTables() {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();

        List<SqlTableSchema> res = new ArrayList<>();

        for (String mapName : context.getMapContainers().keySet()) {
            BiTuple<Data, Object> sample = getSample(context, mapName);

            if (sample == null) {
                continue;
            }

            // TODO: This creates a container. Avoid!
            SqlTableSchema schema = resolveFromSample(
                mapName,
                nodeEngine.getHazelcastInstance().getMap(mapName),
                sample.element1(),
                sample.element2()
            );

            if (schema == null) {
                continue;
            }

            res.add(schema);
        }

        return res;
    }

    private BiTuple<Data, Object> getSample(MapServiceContext context, String mapName) {
        RecordConsumer recordConsumer = new RecordConsumer();

        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            RecordStore<?> recordStore = partitionContainer.getRecordStore(mapName);

            try {
                recordStore.forEach(recordConsumer, true, true);
            } catch (Exception e) {
                // TODO: Get rid of this in favor of iterator.
                // No-op.
            }

            if (recordConsumer.isDone()) {
                break;
            }
        }

        if (recordConsumer.isDone()) {
            return BiTuple.of(recordConsumer.getKeyData(), recordConsumer.getValue());
        } else {
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    private static class RecordConsumer implements BiConsumer<Data, Record> {
        private Data keyData;
        private Object value;

        private boolean done;

        @Override
        public void accept(Data data, Record record) {
            if (done) {
                throw new IllegalStateException("Finished");
            }

            keyData = data;
            value = record.getValue();

            done = true;
        }

        public Data getKeyData() {
            return keyData;
        }

        public Object getValue() {
            return value;
        }

        public boolean isDone() {
            return done;
        }
    }
}
