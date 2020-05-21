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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.locksupport.LockWaitNotifyKeySet;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.map.impl.DataCollection;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class GetAllOperation extends AbstractMultiMapOperation implements PartitionAwareOperation,
        BlockingOperation, ReadonlyOperation {
    private Collection<Data> keys = new ArrayList<>();

    public GetAllOperation() {
    }

    public GetAllOperation(String name, Collection<Data> keys) {
        super(name);
        this.keys = keys;
    }

    //NB: generally based on Map.GetAllOperation#runInternal
    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        int partitionId = getPartitionId();
        MapEntries entries = new MapEntries(1);
        for (Data key : keys) {
            //FIXME: do we actually need this extra partition check?
            if (partitionId == partitionService.getPartitionId(key)) {
                MultiMapValue multiMapValue = container.getMultiMapValueOrNull(key);

                if (multiMapValue != null) {
                    multiMapValue.incrementHit();

                    Collection<MultiMapRecord> coll = multiMapValue.getCollection(executedLocally());
                    Collection<Data> objColl = new ArrayList<>();
                    //strip the MMR
                    for (MultiMapRecord mmr : coll) {
                        Data d = isBinary() ? (Data) mmr.getObject() : toData(mmr.getObject());
                        objColl.add(d);
                    }
                    entries.add(toData(key), toData(new DataCollection(objColl)));
                }
                //TODO: add debug or flag notification on the else?
            }
            //TODO: add debug or flag notification on the else?
        }
        response = entries;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKeySet(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), keys);
    }

    @Override
    public boolean shouldWait() {
        MultiMapContainer container = getOrCreateContainer();
        for (Data dataKey : keys) {
            if (container.isTransactionallyLocked(dataKey)) {
                //FIXME: unsure about this
                return !container.canAcquireLock(dataKey, getCallerUuid(), -1);
            }
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.GET_ALL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (keys == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(keys.size());
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > -1) {
            for (int i = 0; i < size; i++) {
                Data data = IOUtil.readData(in);
                keys.add(data);
            }
        }
    }
}
