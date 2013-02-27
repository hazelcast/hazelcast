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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.logging.Level;

public class CommitOperation extends BaseTxOperation {

    public CommitOperation() {
    }

    public CommitOperation(String txnId, String[] services) {
        super(txnId, services);
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        TransactionManagerService txService = getService();
        txService.commit(getCallerUuid(), txnId, getPartitionId(), services);
        final ILogger logger = nodeEngine.getLogger(getClass());
        for (String serviceName : services) {
            final TransactionalService service = nodeEngine.getService(serviceName);
            if (service == null) {
                logger.log(Level.WARNING, "Unknown service: " + serviceName);
                continue;
            }
            try {
                service.commit(txnId, getPartitionId());
            } catch (Throwable e) {
                logger.log(Level.WARNING, "Problem while service["
                        + serviceName + "] committing the transaction[" + txnId + "]!", e);
            }
        }
    }
}
