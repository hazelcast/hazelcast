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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.function.BiConsumer;

/**
 * Schema resolver for partitioned maps.
 */
public class PartitionedMapSqlSchemaResolver extends MapSqlSchemaResolver {
    public PartitionedMapSqlSchemaResolver(InternalSerializationService ss) {
        super(ss);
    }

    @Override
    protected String getSchemaName() {
        return SqlSchemaResolver.SCHEMA_NAME_PARTITIONED;
    }

    @Override
    protected BiTuple<Data, Object> getSample(DistributedObject object) {
        if (object instanceof MapProxyImpl) {
            return getSample0((MapProxyImpl<?, ?>) object);
        } else {
            return null;
        }
    }

    private BiTuple<Data, Object> getSample0(MapProxyImpl<?, ?> map) {
        RecordConsumer recordConsumer = new RecordConsumer();

        for (PartitionContainer partitionContainer : map.getMapServiceContext().getPartitionContainers()) {
            RecordStore<?> recordStore = partitionContainer.getRecordStore(map.getName());

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
