/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ClearRemoteTransactionBackupOperation extends Operation implements BackupOperation {

    private SerializableXID xid;

    public ClearRemoteTransactionBackupOperation() {
    }

    public ClearRemoteTransactionBackupOperation(SerializableXID xid) {
        this.xid = xid;
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        xaService.removeTransactions(xid);
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(xid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        xid = in.readObject();
    }
}
