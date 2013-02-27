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

package com.hazelcast.transaction;

import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class PrepareOperation extends BaseTxOperation {

    public PrepareOperation() {
    }

    public PrepareOperation(String txnId, String[] services) {
        super(txnId, services);
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        TransactionManagerService txService = getService();
        txService.prepare(getCallerUuid(), txnId, getPartitionId(), services);
        for (String serviceName : services) {
            final TransactionalService service = nodeEngine.getService(serviceName);
            if (service == null) {
                throw new TransactionException("Unknown service: " + serviceName);
            }
            service.prepare(txnId, getPartitionId());
        }
    }
}
