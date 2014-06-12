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

package com.hazelcast.transaction.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import static com.hazelcast.transaction.impl.Transaction.State;

@Beta
public class XAResourceImpl implements XAResource {

    private final TransactionManagerServiceImpl transactionManager;

    private final TransactionContextImpl transactionContext;
    private final ILogger logger;

    private int transactionTimeoutSeconds;

    public XAResourceImpl(TransactionManagerServiceImpl transactionManager,
                          TransactionContextImpl transactionContext, NodeEngineImpl nodeEngine) {
        this.transactionManager = transactionManager;
        this.transactionContext = transactionContext;
        this.logger = nodeEngine.getLogger(XAResourceImpl.class);
    }

    //XAResource --START
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
                    final Transaction transaction = getTransaction();
                    transactionManager.addManagedTransaction(xid, transaction);
                    transaction.begin();
                } catch (IllegalStateException e) {
                    logger.severe(e);
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
        final TransactionImpl transaction = (TransactionImpl) getTransaction();
        final SerializableXID sXid = transaction.getXid();
        if (sXid == null || !sXid.equals(xid)) {
            logger.severe("started xid: " + sXid + " and given xid : " + xid + " not equal!!!");
        }
        validateTx(transaction, State.ACTIVE);

        switch (flags) {
            case XAResource.TMSUCCESS:
                //successfully end.
                break;
            case XAResource.TMFAIL:
                transaction.setRollbackOnly();
                throw new XAException(XAException.XA_RBROLLBACK);
//                break;
            case XAResource.TMSUSPEND:
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
    }

    @Override
    public synchronized int prepare(Xid xid) throws XAException {
        nullCheck(xid);
        final TransactionImpl transaction = (TransactionImpl) getTransaction();
        final SerializableXID sXid = transaction.getXid();
        if (sXid == null || !sXid.equals(xid)) {
            logger.severe("started xid: " + sXid + " and given xid : " + xid + " not equal!!!");
        }
        validateTx(transaction, State.ACTIVE);

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

        final Transaction transaction = getTransaction(xid);

        if (onePhase) {
            validateTx(transaction, State.ACTIVE);
            transaction.prepare();
        }

        validateTx(transaction, State.PREPARED);

        try {
            transaction.commit();
            transactionManager.removeManagedTransaction(xid);
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    @Override
    public synchronized void rollback(Xid xid) throws XAException {
        nullCheck(xid);
        final Transaction transaction = getTransaction(xid);
        //NO_TXN means do not validate state
        validateTx(transaction, State.NO_TXN);
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
        if (xaResource instanceof XAResourceImpl) {
            XAResourceImpl other = (XAResourceImpl) xaResource;
            return transactionManager.getGroupName().equals(other.transactionManager.getGroupName());
        } else {
            // since XaResourceProxy is in client package and client package has dependency on ours,
            // we give our resource to them to check whether they are the same.
            return xaResource.isSameRM(this);
        }
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

    //XAResource --END

    public String getGroupName() {
        return transactionManager.getGroupName();
    }

    private void nullCheck(Xid xid) throws XAException {
        if (xid == null) {
            final XAException xaException = new XAException(XAException.XAER_INVAL);
            logger.severe("Xid cannot be null!!!", xaException);
            throw xaException;
        }
    }

    private void validateTx(Transaction tx, State state) throws XAException {
        if (tx == null) {
            final XAException xaException = new XAException(XAException.XAER_NOTA);
            logger.severe("Transaction is not available!!!", xaException);
            throw xaException;
        }
        final State txState = tx.getState();
        switch (state) {
            case ACTIVE:
                if (txState != State.ACTIVE) {
                    final XAException xaException = new XAException(XAException.XAER_NOTA);
                    logger.severe("Transaction is not active!!! state: " + txState, xaException);
                    throw xaException;
                }
                break;
            case PREPARED:
                if (txState != State.PREPARED) {
                    final XAException xaException = new XAException(XAException.XAER_INVAL);
                    logger.severe("Transaction is not prepared!!! state: " + txState, xaException);
                    throw xaException;
                }
                break;
            default:
                break;
        }
    }

    private Transaction getTransaction(Xid xid) {
        return transactionManager.getManagedTransaction(xid);
    }

    private Transaction getTransaction() {
        return transactionContext.getTransaction();
    }

    @Override
    public String toString() {
        final String txnId = transactionContext.getTxnId();
        final StringBuilder sb = new StringBuilder("XAResourceImpl{");
        sb.append("txdId=").append(txnId);
        sb.append(", transactionTimeoutSeconds=").append(transactionTimeoutSeconds);
        sb.append('}');
        return sb.toString();
    }
}
