/*
 * Copyright (c) 2008, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockStoreImpl;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

public class EntrySetUnlockOperation extends UnlockOperation {

    protected String name;
    protected Data value;
    protected boolean onlyUnlock;
    protected String caller;

    public EntrySetUnlockOperation() {
    }

    public EntrySetUnlockOperation(ObjectNamespace namespace, String name, Data key, Data value,
                                   boolean onlyUnlock, String caller, long threadId) {

        super(namespace, key, threadId);
        this.name = name;
        this.value = value;
        this.onlyUnlock = onlyUnlock;
        this.caller = caller;
    }

    @Override
    public void run() throws Exception {
        if (!onlyUnlock) {
            getRecordStore().set(key, value, 0);
        }

        LockStoreImpl lockStore = getLockStore();
        boolean unlocked = lockStore.unlock(key, caller, threadId, getReferenceCallId());
        response = unlocked;
        if (!unlocked) {
            // we can not check for retry here, hence just throw the exception
            String ownerInfo = lockStore.getOwnerInfo(key);
            throw new IllegalMonitorStateException("Current thread is not owner of the lock! -> " + ownerInfo);
        }
    }

    protected RecordStore getRecordStore() {
        MapService service = getNodeEngine().getService(MapService.SERVICE_NAME);
        return service.getMapServiceContext().getRecordStore(getPartitionId(), name);
    }

    @Override
    public boolean shouldBackup() {
        Record record = getRecordStore().getRecord(key);
        return record != null;
    }

    @Override
    public Operation getBackupOperation() {
        final Record record = getRecordStore().getRecord(key);
        final RecordInfo replicationInfo = buildRecordInfo(record);
        return new PutBackupOperation(name, key, value, replicationInfo, false);
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_SET_UNLOCK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeData(value);
        out.writeBoolean(onlyUnlock);
        out.writeUTF(caller);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        value = in.readData();
        onlyUnlock = in.readBoolean();
        caller = in.readUTF();
    }

}
