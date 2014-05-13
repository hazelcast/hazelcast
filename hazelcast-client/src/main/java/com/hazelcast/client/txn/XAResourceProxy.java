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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.SerializableXID;
import com.hazelcast.transaction.impl.Transaction;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.UUID;

import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;

/**
 * @author ali 14/02/14
 */
public class XAResourceProxy implements XAResource {

    private final TransactionContextProxy transactionContext;
    private final ClientTransactionManager transactionManager;
    private final ILogger logger;
    private final String uuid = UUID.randomUUID().toString();
    //To overcome race condition in Atomikos

    private int transactionTimeoutSeconds;

    public XAResourceProxy(TransactionContextProxy transactionContext) {
        this.transactionContext = transactionContext;
        this.transactionManager = transactionContext.getTransactionManager();
        logger = Logger.getLogger(XAResourceProxy.class);
    }

    @Override
    public synchronized void start(Xid xid, int flags) throws XAException {
        nullCheck(xid);
        switch (flags) {
            case TMNOFLAGS:
                if (getTransaction(xid) != null) {
                    final XAException xaException = new XAException(XAException.XAER_DUPID);
                    logger.severe("Duplicate xid: " + xid, xaException);
                    throw xaException;
                }
                try {
                    final TransactionProxy transaction = getTransaction();
                    transactionManager.addManagedTransaction(xid, transaction);
                    transaction.begin();
                } catch (IllegalStateException e) {
                    throw new XAException(XAException.XAER_INVAL);
                }
                break;
            case TMRESUME:
            case TMJOIN:
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
    }

    @Override
    public synchronized void end(Xid xid, int flags) throws XAException {
        nullCheck(xid);
        final TransactionProxy transaction = getTransaction();
        final SerializableXID sXid = transaction.getXid();
        if (sXid == null || !sXid.equals(xid)) {
            logger.severe("started xid: " + sXid + " and given xid : " + xid + " not equal!!!");
        }
        validateTx(transaction, ACTIVE);

        switch (flags) {
            case XAResource.TMSUCCESS:
                //successfully end.
                break;
            case XAResource.TMFAIL:
                try {
                    transaction.rollback();
                    transactionManager.removeManagedTransaction(xid);
                } catch (IllegalStateException e) {
                    throw new XAException(XAException.XAER_RMERR);
                }
                break;
            case XAResource.TMSUSPEND:
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
    }

    @Override
    public synchronized int prepare(Xid xid) throws XAException {
        nullCheck(xid);
        final TransactionProxy transaction = getTransaction();
        final SerializableXID sXid = transaction.getXid();
        if (sXid == null || !sXid.equals(xid)) {
            logger.severe("started xid: " + sXid + " and given xid : " + xid + " not equal!!!");
        }
        validateTx(transaction, ACTIVE);

        try {
            transaction.prepare();
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
        return XAResource.XA_OK;
    }

    @Override
    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        nullCheck(xid);

        final TransactionProxy transaction = getTransaction(xid);
        if (transaction == null) {
            if (transactionManager.recover(xid, true)) {
                return;
            }
            final XAException xaException = new XAException(XAException.XAER_NOTA);
            logger.severe("Transaction is not available!!!", xaException);
            throw xaException;
        }

        validateTx(transaction, onePhase ? ACTIVE : PREPARED);

        try {
            transaction.commit(onePhase);
            transactionManager.removeManagedTransaction(xid);
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    @Override
    public synchronized void rollback(Xid xid) throws XAException {
        nullCheck(xid);
        final TransactionProxy transaction = getTransaction(xid);
        if (transaction == null) {
            if (transactionManager.recover(xid, false)) {
                return;
            }
            final XAException xaException = new XAException(XAException.XAER_NOTA);
            logger.severe("Transaction is not available!!!", xaException);
            throw xaException;
        }
        validateTx(transaction, Transaction.State.NO_TXN);
        //NO_TXN means do not validate state
        try {
            transaction.rollback();
            transactionManager.removeManagedTransaction(xid);
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    @Override
    public synchronized void forget(Xid xid) throws XAException {
        throw new XAException(XAException.XAER_PROTO);
    }

    @Override
    public synchronized boolean isSameRM(XAResource xaResource) throws XAException {
        if (this == xaResource) {
            return true;
        }
        if (xaResource instanceof XAResourceProxy) {
            XAResourceProxy other = (XAResourceProxy) xaResource;
            return transactionManager.equals(other.transactionManager);
        }
        return false;
    }

    @Override
    public synchronized Xid[] recover(int flag) throws XAException {
        return transactionManager.recover();
    }

    @Override
    public synchronized int getTransactionTimeout() throws XAException {
        return transactionTimeoutSeconds;
    }

    @Override
    public synchronized boolean setTransactionTimeout(int seconds) throws XAException {
        this.transactionTimeoutSeconds = seconds;
        return false;
    }

    private void nullCheck(Xid xid) throws XAException {
        if (xid == null) {
            final XAException xaException = new XAException(XAException.XAER_INVAL);
            logger.severe("Xid cannot be null!!!", xaException);
            throw xaException;
        }
    }

    private TransactionProxy getTransaction() {
        return transactionContext.transaction;
    }

    private TransactionProxy getTransaction(Xid xid) {
        return transactionManager.getManagedTransaction(xid);
    }

    private void validateTx(TransactionProxy tx, Transaction.State state) throws XAException {
        if (tx == null) {
            final XAException xaException = new XAException(XAException.XAER_NOTA);
            logger.severe("Transaction is not available!!!", xaException);
            throw xaException;
        }
        final Transaction.State txState = tx.getState();
        switch (state) {
            case ACTIVE:
                if (txState != ACTIVE) {
                    final XAException xaException = new XAException(XAException.XAER_NOTA);
                    logger.severe("Transaction is not active!!! state: " + txState, xaException);
                    throw xaException;
                }
                break;
            case PREPARED:
                if (txState != Transaction.State.PREPARED) {
                    final XAException xaException = new XAException(XAException.XAER_INVAL);
                    logger.severe("Transaction is not prepared!!! state: " + txState, xaException);
                    throw xaException;
                }
                break;
            default:
                break;
        }
    }

    @Override
    public String toString() {
        final String txnId = transactionContext.getTxnId();
        final StringBuilder sb = new StringBuilder("XAResourceImpl{");
        sb.append("uuid=").append(uuid);
        sb.append("txdId=").append(txnId);
        sb.append(", transactionTimeoutSeconds=").append(transactionTimeoutSeconds);
        sb.append('}');
        return sb.toString();
    }

}
