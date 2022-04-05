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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class EntryRecordStoreLoader extends BasicRecordStoreLoader {

    EntryRecordStoreLoader(RecordStore recordStore) {
        super(recordStore);
    }

    /**
     * Transforms a map to a list of serialised key-value-expirationTime sequences.
     *
     * @param entries the map to be transformed
     * @return the list of serialised alternating key-value pairs
     */
    protected List<Data> getLoadingSequence(Map<?, ?> entries) {
        List<Data> keyValueSequence = new ArrayList<>(entries.size() * 2);
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            Object key = entry.getKey();
            MetadataAwareValue loaderEntry = (MetadataAwareValue) entry.getValue();
            Object value = loaderEntry.getValue();
            long expirationTime = loaderEntry.getExpirationTime();
            Data dataKey = mapServiceContext.toData(key);
            Data dataValue = mapServiceContext.toData(value);
            Data dataExpirationTime = mapServiceContext.toData(expirationTime);
            keyValueSequence.add(dataKey);
            keyValueSequence.add(dataValue);
            keyValueSequence.add(dataExpirationTime);
        }
        return keyValueSequence;
    }

    protected Operation createOperation(List<Data> loadingSequence) {
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(name);
        return operationProvider.createPutFromLoadAllOperation(name, loadingSequence, true);
    }
}
