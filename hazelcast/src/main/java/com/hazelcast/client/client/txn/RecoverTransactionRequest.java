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

package com.hazelcast.client.client.txn;

import com.hazelcast.client.client.CallableClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;
import java.security.Permission;

public class RecoverTransactionRequest extends CallableClientRequest {

    private boolean commit;
    private SerializableXID sXid;

    public RecoverTransactionRequest() {
    }

    public RecoverTransactionRequest(SerializableXID sXid, boolean commit) {
        this.sXid = sXid;
        this.commit = commit;
    }

    @Override
    public Object call() throws Exception {
        TransactionManagerServiceImpl service = getService();
        service.recoverClientTransaction(sXid, commit);
        return null;
    }

    @Deprecated
    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.RECOVER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeBoolean("c", commit);
        ObjectDataOutput out = writer.getRawDataOutput();
        sXid.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        commit = reader.readBoolean("c");
        ObjectDataInput in = reader.getRawDataInput();
        sXid = new SerializableXID();
        sXid.readData(in);
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
