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
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;

import java.util.Collection;
import java.util.Iterator;

/**
 * Schema resolver for replicated maps.
 */
public class ReplicatedMapSqlSchemaResolver extends MapSqlSchemaResolver {
    public ReplicatedMapSqlSchemaResolver(InternalSerializationService ss) {
        super(ss);
    }

    @Override
    protected String getSchemaName() {
        return SqlSchemaResolver.SCHEMA_NAME_REPLICATED;
    }

    @Override
    protected BiTuple<Data, Object> getSample(DistributedObject object) {
        if (object instanceof ReplicatedMapProxy) {
            return getSample0((ReplicatedMapProxy<?, ?>) object);
        } else {
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    private BiTuple<Data, Object> getSample0(ReplicatedMapProxy<?, ?> map) {
        Collection<ReplicatedRecordStore> stores = map.getService().getAllReplicatedRecordStores(map.getName());

        for (ReplicatedRecordStore store : stores) {
            Iterator<ReplicatedRecord> iterator = store.recordIterator();

            if (!iterator.hasNext()) {
                continue;
            }

            ReplicatedRecord<?, ?> record = iterator.next();

            Object key = record.getKey();
            Object value = record.getValue();

            if (!(key instanceof Data)) {
                // TODO: Refactor code to avoid that double serialization/deserialization. Our code should be able to work with
                //  objects in the same way it works with data!
                key = ss.toData(key);
            }

            return BiTuple.of((Data) key, value);
        }

        return null;
    }
}
