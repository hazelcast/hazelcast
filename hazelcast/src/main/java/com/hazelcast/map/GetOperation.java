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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class GetOperation extends AbstractMapOperation implements IdentifiedDataSerializable {

    private transient Data result;
    private transient MapService mapService;

    public GetOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public GetOperation() {
    }

    public void run() {
        mapService = (MapService) getService();
        RecordStore recordStore = mapService.getRecordStore(getPartitionId(), name);
        if (getTxnId() != null) {
            String txnId = getTxnId();
            PartitionContainer p = mapService.getPartitionContainer(getPartitionId());
            final TransactionLog log = p.getTransactionLog(txnId);
            final TransactionLogItem logItem = log.getTransactionLogItem(dataKey);
            if (logItem != null) {
                if (!logItem.isRemoved()) {
                    result = logItem.getValue();
                }
                return;
            }
        }
        result = mapService.toData(recordStore.get(dataKey));
    }

    public void afterRun() {
        mapService.interceptAfterProcess(name, MapOperationType.GET, dataKey, result, result);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String toString() {
        return "GetOperation{" +
                '}';
    }

    public int getId() {
        return DataSerializerMapHook.GET;
    }
}
