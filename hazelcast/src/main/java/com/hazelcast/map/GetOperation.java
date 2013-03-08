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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

public class GetOperation extends KeyBasedMapOperation implements IdentifiedDataSerializable {

    private String txnId;
    private transient Data result;

    public GetOperation() {
    }

    public GetOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public GetOperation(String name, Data dataKey, String txnId) {
        super(name, dataKey);
        this.txnId = txnId;
    }

    public void run() {
        if (txnId != null) {
            final TransactionItem item = partitionContainer.getTransactionItem(new TransactionKey(txnId, name, dataKey));
            if (item != null) {
                result = item.getValue();
                return;
            }
        }
        result = mapService.toData(recordStore.get(dataKey));
    }

    public void afterRun() {
        mapService.interceptAfterProcess(name, MapOperationType.GET, dataKey, result, result);
        mapContainer.getMapOperationCounter().incrementGets(Clock.currentTimeMillis() - getStartTime());
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String toString() {
        return "GetOperation{}";
    }

    public int getId() {
        return DataSerializerMapHook.GET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        txnId = in.readUTF();
    }
}
