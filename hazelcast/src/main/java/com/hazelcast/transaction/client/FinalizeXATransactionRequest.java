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

package com.hazelcast.transaction.client;

import com.hazelcast.client.impl.client.PartitionClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.xa.FinalizeRemoteTransactionOperation;
import com.hazelcast.transaction.impl.xa.XAService;

import java.io.IOException;
import java.security.Permission;

public class FinalizeXATransactionRequest extends PartitionClientRequest {

    private Data xidData;
    private boolean isCommit;

    public FinalizeXATransactionRequest() {
    }

    public FinalizeXATransactionRequest(Data xidData, boolean isCommit) {
        this.xidData = xidData;
        this.isCommit = isCommit;
    }

    @Override
    protected Operation prepareOperation() {
        return new FinalizeRemoteTransactionOperation(xidData, isCommit);
    }

    @Override
    protected int getPartition() {
        return getClientEngine().getPartitionService().getPartitionId(xidData);
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.FINALIZE_XA;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        super.write(writer);
        writer.writeBoolean("c", isCommit);
        ObjectDataOutput rawDataOutput = writer.getRawDataOutput();
        rawDataOutput.writeData(xidData);

    }

    @Override
    public void read(PortableReader reader) throws IOException {
        super.read(reader);
        isCommit = reader.readBoolean("c");
        ObjectDataInput rawDataInput = reader.getRawDataInput();
        xidData = rawDataInput.readData();
    }
}
