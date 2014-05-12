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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.MapService;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;

public class GetEntryViewOperation extends KeyBasedMapOperation {

    private EntryView<Data, Data> result;

    public GetEntryViewOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public GetEntryViewOperation() {
    }

    public void run() {
        MapService mapService = getService();
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        Record record = recordStore.getRecord(dataKey);
        if (record != null) {
            result = mapService.createSimpleEntryView(record.getKey(), mapService.toData(record.getValue()), record);
        }
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String toString() {
        return "GetEntryViewOperation{" +
                '}';
    }

}
