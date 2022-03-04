/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl.operations;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.transaction.impl.TransactionDataSerializerHook.BROADCAST_TX_ROLLBACK;

public final class BroadcastTxRollbackOperation extends AbstractTxOperation {

    private UUID txnId;

    public BroadcastTxRollbackOperation() {
    }

    public BroadcastTxRollbackOperation(UUID txnId) {
        this.txnId = txnId;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Collection<TransactionalService> services = nodeEngine.getServices(TransactionalService.class);
        for (TransactionalService service : services) {
            try {
                service.rollbackTransaction(txnId);
            } catch (Exception e) {
                getLogger().warning("Error while rolling back transaction: " + txnId, e);
            }
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public int getClassId() {
        return BROADCAST_TX_ROLLBACK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = UUIDSerializationUtil.readUUID(in);
    }
}
