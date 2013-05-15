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

package com.hazelcast.map.tx;

import com.hazelcast.map.*;
import com.hazelcast.map.BasePutOperation;
import com.hazelcast.map.PutBackupOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * @mdogan 3/25/13
 */
public class TxnSetOperation extends BasePutOperation implements MapTxnOperation {

    private long version;
    private boolean shouldBackup = false;

    public TxnSetOperation() {
    }

    public TxnSetOperation(String name, Data dataKey, Data value, long ttl, long version) {
        super(name, dataKey, value, ttl);
        this.version = version;
    }


    @Override
    public void run() {
        recordStore.unlock(dataKey, getCallerUuid(), getThreadId());
        Record record = recordStore.getRecords().get(dataKey);
        if (record == null || version == record.getVersion()){
            recordStore.set(dataKey, dataValue, ttl);
            shouldBackup = true;
        }
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    public boolean shouldNotify() {
        return true;
    }

    public Operation getBackupOperation() {
        return new PutBackupOperation(name, dataKey, dataValue, ttl, true);
    }

    public void onWaitExpire() {
        final ResponseHandler responseHandler = getResponseHandler();
        responseHandler.sendResponse(false);
    }

    @Override
    public boolean shouldBackup() {
        return shouldBackup;
    }

    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(version);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        version = in.readLong();
    }
}
