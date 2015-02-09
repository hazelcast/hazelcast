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

package com.hazelcast.spring.context;

import org.springframework.stereotype.Component;
import org.springframework.transaction.*;
import org.springframework.transaction.support.SimpleTransactionStatus;

/**
 * @leimer 8/14/12
 */

@Component
public class DummyTransactionManager implements PlatformTransactionManager {

    private volatile boolean committed = false;

    public boolean isCommitted() {
        return committed;
    }

    public TransactionStatus getTransaction(TransactionDefinition transactionDefinition) throws TransactionException {
        committed = false;
        return new SimpleTransactionStatus(true);
    }

    public void commit(TransactionStatus transactionStatus) throws TransactionException {
        if (committed) {
            throw new IllegalTransactionStateException("Transaction should not be committed at this stage!");
        }
        committed = true;
    }

    public void rollback(TransactionStatus transactionStatus) throws TransactionException {
        throw new UnexpectedRollbackException("We do not expect rollback!");
    }
}
