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

package com.hazelcast.spring.transaction;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} implementation
 * for a single {@link HazelcastInstance}. Binds a Hazelcast {@link TransactionContext}
 * from the instance to the thread (as it is already bounded by Hazelcast itself) and makes it available for access.
 * <p>
 * <i>Note:</i> This transaction manager doesn't supports nested transactions, since Hazelcast doesn't support them either.
 *
 * @author Balint Krivan
 * @see #getTransactionContext(HazelcastInstance)
 * @see #getTransactionContext()
 */
public class HazelcastTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

    private HazelcastInstance hazelcastInstance;

    public HazelcastTransactionManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * Returns the transaction context for the given Hazelcast instance bounded to the current thread.
     *
     * @throws NoTransactionException if the transaction context cannot be found
     */
    public static TransactionContext getTransactionContext(HazelcastInstance hazelcastInstance) {
        TransactionContextHolder transactionContextHolder =
                (TransactionContextHolder) TransactionSynchronizationManager.getResource(hazelcastInstance);
        if (transactionContextHolder == null) {
            throw new NoTransactionException("No TransactionContext with actual transaction available for current thread");
        }
        return transactionContextHolder.getContext();
    }

    /**
     * Returns the transaction context
     *
     * @throws NoTransactionException if the transaction context cannot be found
     */
    public TransactionContext getTransactionContext() {
        return getTransactionContext(this.hazelcastInstance);
    }

    @Override
    public Object getResourceFactory() {
        return hazelcastInstance;
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        HazelcastTransactionObject txObject = new HazelcastTransactionObject();

        TransactionContextHolder transactionContextHolder =
                (TransactionContextHolder) TransactionSynchronizationManager.getResource(hazelcastInstance);
        if (transactionContextHolder != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found thread-bound TransactionContext [" + transactionContextHolder.getContext() + "]");
            }
            txObject.setTransactionContextHolder(transactionContextHolder, false);
        }

        return txObject;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return ((HazelcastTransactionObject) transaction).hasTransaction();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        HazelcastTransactionObject txObject = (HazelcastTransactionObject) transaction;

        try {
            if (txObject.getTransactionContextHolder() == null) {
                TransactionContext transactionContext = hazelcastInstance.newTransactionContext();
                if (logger.isDebugEnabled()) {
                    logger.debug("Opened new TransactionContext [" + transactionContext + "]");
                }
                txObject.setTransactionContextHolder(new TransactionContextHolder(transactionContext), true);
            }

            txObject.getTransactionContextHolder().beginTransaction();

            if (txObject.isNewTransactionContextHolder()) {
                TransactionSynchronizationManager.bindResource(hazelcastInstance, txObject.getTransactionContextHolder());
            }
        } catch (Throwable ex) {
            closeTransactionContextAfterFailedBegin(txObject);
            throw new CannotCreateTransactionException("Could not begin Hazelcast transaction", ex);
        }
    }

    private void closeTransactionContextAfterFailedBegin(HazelcastTransactionObject txObject) {
        if (txObject.isNewTransactionContextHolder()) {
            TransactionContext context = txObject.getTransactionContextHolder().getContext();
            try {
                context.rollbackTransaction();
            } catch (Throwable ex) {
                logger.debug("Could not rollback Hazelcast transaction after failed transaction begin", ex);
            }
            txObject.setTransactionContextHolder(null, false);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        HazelcastTransactionObject txObject = (HazelcastTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Committing Hazelcast transaction on TransactionContext ["
                    + txObject.getTransactionContextHolder().getContext() + "]");
        }

        try {
            txObject.getTransactionContextHolder().getContext().commitTransaction();
        } catch (com.hazelcast.transaction.TransactionException ex) {
            throw new TransactionSystemException("Could not commit Hazelcast transaction", ex);
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        HazelcastTransactionObject txObject = (HazelcastTransactionObject) status.getTransaction();
        txObject.setRollbackOnly(true);
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        HazelcastTransactionObject txObject = (HazelcastTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Rolling back Hazelcast transaction on TransactionContext ["
                    + txObject.getTransactionContextHolder().getContext() + "]");
        }

        TransactionContext tx = txObject.getTransactionContextHolder().getContext();
        tx.rollbackTransaction();
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        HazelcastTransactionObject txObject = (HazelcastTransactionObject) transaction;

        if (txObject.isNewTransactionContextHolder()) {
            TransactionSynchronizationManager.unbindResourceIfPossible(hazelcastInstance);
        }

        txObject.getTransactionContextHolder().clear();
    }

    private static class HazelcastTransactionObject implements SmartTransactionObject {

        private TransactionContextHolder transactionContextHolder;
        private boolean newTransactionContextHolder;
        private boolean rollbackOnly;

        void setRollbackOnly(boolean rollbackOnly) {
            this.rollbackOnly = rollbackOnly;
        }

        void setTransactionContextHolder(TransactionContextHolder transactionContextHolder,
                                         boolean newTransactionContextHolder) {
            this.transactionContextHolder = transactionContextHolder;
            this.newTransactionContextHolder = newTransactionContextHolder;
        }

        TransactionContextHolder getTransactionContextHolder() {
            return transactionContextHolder;
        }

        boolean isNewTransactionContextHolder() {
            return newTransactionContextHolder;
        }

        boolean hasTransaction() {
            return this.transactionContextHolder != null && this.transactionContextHolder.isTransactionActive();
        }

        @Override
        public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        @Override
        public void flush() {
            // do nothing here
        }
    }
}
