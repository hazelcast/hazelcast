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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.security.permission.TransactionPermission;

import java.security.Permission;

public class RollbackTransactionRequest extends BaseTransactionRequest {

    public RollbackTransactionRequest() {
    }

    @Override
    public Object innerCall() throws Exception {
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.getTransactionContext(txnId).rollbackTransaction();
        endpoint.removeTransactionContext(txnId);
        return null;
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.ROLLBACK;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
