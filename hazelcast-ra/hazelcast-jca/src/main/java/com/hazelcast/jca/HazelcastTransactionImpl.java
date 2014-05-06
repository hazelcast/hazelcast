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

package com.hazelcast.jca;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.transaction.xa.XAException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class HazelcastTransactionImpl extends JcaBase implements HazelcastTransaction {
    /** List of former transaction used during transaction restore */
    //private static final ConcurrentMap<CallContext,CallContext> predecessors=new ConcurrentHashMap<CallContext,CallContext>();

    /**
     * access to the creator of this {@link #connection}
     */
    private final ManagedConnectionFactoryImpl factory;
    /**
     * access to the creator of this transaction
     */
    private final ManagedConnectionImpl connection;
    /**
     * The hazelcast transaction context itself
     */
    private TransactionContext txContext;

    public HazelcastTransactionImpl(ManagedConnectionFactoryImpl factory, ManagedConnectionImpl connection) {
        this.setLogWriter(factory.getLogWriter());

        this.factory = factory;
        this.connection = connection;
    }

    /**
     * Delegates the hazelcast instance access to the @{link #connection}
     *
     * @see ManagedConnectionImpl#getHazelcastInstance()
     */
    private HazelcastInstance getHazelcastInstance() {
        return connection.getHazelcastInstance();
    }

    /**
     * Delegates the connection event propagation to the @{link #connection}
     *
     * @see ManagedConnectionImpl#fireConnectionEvent(int)
     */
    private void fireConnectionEvent(int event) {
        connection.fireConnectionEvent(event);
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.LocalTransaction#begin()
     */
    public void begin() throws ResourceException {
        if (null == txContext) {
            factory.logHzConnectionEvent(this, HzConnectionEvent.TX_START);

            this.txContext = getHazelcastInstance().newTransactionContext();
            this.connection.getTx().setTxContext(txContext);

            log(Level.FINEST, "begin");
            txContext.beginTransaction();

            fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_STARTED);

        } else {
            log(Level.INFO, "Ignoring duplicate TX begin event");
        }
    }

    /* (non-Javadoc)
     * @see javax.resource.cci.LocalTransaction#commit()
     */
    public void commit() throws ResourceException {
        factory.logHzConnectionEvent(this, HzConnectionEvent.TX_COMPLETE);

        log(Level.FINEST, "commit");
        if (this.txContext != null) {
            this.txContext.commitTransaction();
            fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
            this.txContext = null;
        } else {
            throw new ResourceException("Invalid transaction context; "
                    + "commit operation invoked without an active transaction context");
        }
    }


    /* (non-Javadoc)
     * @see javax.resource.cci.LocalTransaction#rollback()
     */
    public void rollback() throws ResourceException {
        factory.logHzConnectionEvent(this, HzConnectionEvent.TX_COMPLETE);

        log(Level.FINEST, "rollback");
        if (this.txContext != null) {
            this.txContext.rollbackTransaction();
            fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
            this.txContext = null;
        } else {
            throw new ResourceException("Invalid transaction context; "
                    + "rollback operation invoked without an active transaction context");
        }
    }

    public TransactionContext getTxContext() {
        return this.txContext;
    }

    public void setTxContext(TransactionContext txContext) {
        this.txContext = txContext;
    }

    public static TransactionContext createTransaction(int timeout, HazelcastInstance hazelcastInstance) throws XAException {
        final TransactionOptions transactionOptions = TransactionOptions.getDefault().setTimeout(timeout, TimeUnit.SECONDS);
        return hazelcastInstance.newTransactionContext(transactionOptions);
    }

}
