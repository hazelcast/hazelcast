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

package com.hazelcast.client.txn;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;

/**
 * @author ali 17/02/14
 */
public class RecoverTransactionRequest extends CallableClientRequest implements Portable {

    boolean commit;

    SerializableXID sXid;

    public RecoverTransactionRequest() {
    }

    public RecoverTransactionRequest(SerializableXID sXid, boolean commit) {
        this.sXid = sXid;
        this.commit = commit;
    }

    @Override
    public Object call() throws Exception {
        final TransactionManagerServiceImpl service = getService();
        service.recoverClientTransaction(sXid, commit);
        return null;
    }

    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.RECOVER;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("c", commit);
        final ObjectDataOutput out = writer.getRawDataOutput();
        sXid.writeData(out);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        commit = reader.readBoolean("c");
        final ObjectDataInput in = reader.getRawDataInput();
        sXid = new SerializableXID();
        sXid.readData(in);


    }
}
