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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

/**
 * @ali 1/22/13
 */
public class AcquireOperation extends SemaphoreBackupAwareOperation implements WaitSupport {

    long timeout;

    public AcquireOperation() {
    }

    public AcquireOperation(String name, int permitCount, long timeout) {
        super(name, permitCount);
        this.timeout = timeout;
    }

    public void run() throws Exception {
        Permit permit = getPermit();
        response = permit.acquire(permitCount, getCallerUuid());
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timeout);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timeout = in.readLong();
    }

    public WaitNotifyKey getWaitKey() {
        return new SemaphoreWaitNotifyKey(name, "acquire");
    }

    public boolean shouldWait() {
        Permit permit = getPermit();
        return timeout != 0 && !permit.isAvailable(permitCount);
    }

    public long getWaitTimeoutMillis() {
        return timeout;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new AcquireBackupOperation(name, permitCount, getCallerUuid());
    }
}
