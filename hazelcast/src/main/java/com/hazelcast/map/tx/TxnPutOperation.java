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

import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PutOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * @mdogan 3/25/13
 */
public class TxnPutOperation extends PutOperation implements MapTxnOperation {

    private long version;

    public TxnPutOperation() {
    }

    public TxnPutOperation(String name, Data dataKey, Data value, long ttl) {
        super(name, dataKey, value, ttl);
    }

    @Override
    public void run() {
        // if version == record.version {
            mapService.toData(recordStore.put(dataKey, dataValue, ttl));
            recordStore.forceUnlock(dataKey);
        // }
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

    public WaitNotifyKey getNotifiedKey() {
        return new LockWaitNotifyKey(new LockNamespace(MapService.SERVICE_NAME, name), dataKey);
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
