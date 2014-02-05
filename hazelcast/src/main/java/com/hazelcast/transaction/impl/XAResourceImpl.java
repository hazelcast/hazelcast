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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.transaction.TransactionException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

@Beta
public class XAResourceImpl implements XAResource {

    private final TransactionContextImpl transactionContext;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private int transactionTimeoutSeconds;

    private Xid xid = null;

    public XAResourceImpl(TransactionContextImpl transactionContext, NodeEngineImpl nodeEngine) {
        this.transactionContext = transactionContext;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(XAResourceImpl.class);
    }

    //XAResource --START
    @Override
    public synchronized void start(Xid xid, int flags) throws XAException {
        validateXID(xid);
        switch (flags) {
            case TMNOFLAGS:
                if (this.xid != null) {
                    final XAException xaException = new XAException(XAException.XAER_DUPID);
                    logger.severe("Duplicate xid: " + xid + ", this.xid: " + this.xid, xaException);
                    throw xaException;
                }
                this.xid = xid;
                try {
                    transactionContext.beginTransaction();
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
        validateXID(xid);
        validateTx(xid, Transaction.State.ACTIVE);

        switch (flags) {
            case XAResource.TMSUCCESS:
                //successfully end.
                break;
            case XAResource.TMFAIL:
                try {
                    transactionContext.rollbackTransaction();
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
        validateXID(xid);

        validateTx(xid, Transaction.State.ACTIVE);

        final Transaction tx = getTransaction();
        try {
            tx.prepare();
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
        return XAResource.XA_OK;
    }

    @Override
    public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
        validateXID(xid);

        final Transaction tx = getTransaction();

        if (onePhase) {
            validateTx(xid, Transaction.State.ACTIVE);
            tx.prepare();
        }

        validateTx(xid, Transaction.State.PREPARED);

        try {
            tx.commit();
        } catch (TransactionException e) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    @Override
    public synchronized void rollback(Xid xid) throws XAException {
        validateXID(xid);
        validateTx(xid, Transaction.State.NO_TXN);
        try {
            transactionContext.rollbackTransaction();
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
            final HazelcastInstance otherInstance = other.nodeEngine.getHazelcastInstance();
            return nodeEngine.getHazelcastInstance().equals(otherInstance);
        }
        return false;
    }

    @Override
    public synchronized Xid[] recover(int flag) throws XAException {
        final Transaction tx = getTransaction();
        if (this.xid != null && tx.getState() == Transaction.State.PREPARED) {
            return new Xid[]{this.xid};
        }
        return new Xid[0];
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

    private Transaction getTransaction() {
        return transactionContext.getTransaction();
    }

    private void validateXID(Xid xid) throws XAException {
        if (xid == null) {
            final XAException xaException = new XAException(XAException.XAER_INVAL);
            logger.severe("Xid cannot be null!!!", xaException);
            throw xaException;
        }
    }

    private void validateTx(Xid xid, Transaction.State state) throws XAException {
        final Transaction tx = getTransaction();
        if (this.xid == null) {
            final XAException xaException = new XAException(XAException.XAER_NOTA);
            logger.severe("Not yet started!!! xid: " + xid, xaException);
            throw xaException;
        }
        if (!this.xid.equals(xid)) {
            final XAException xaException = new XAException(XAException.XAER_DUPID);
            logger.severe("Duplicate xid: " + xid + ", this.xid: " + this.xid, xaException);
            throw xaException;
        }
        final Transaction.State txState = tx.getState();
        switch (state) {
            case ACTIVE:
                if (txState != Transaction.State.ACTIVE) {
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
            case NO_TXN:
                //this means do not check state
                break;
        }
    }

    @Override
    public String toString() {
        final String txnId = transactionContext.getTxnId();
        final StringBuilder sb = new StringBuilder("XAResourceImpl{");
        sb.append("txdId=").append(txnId);
        sb.append(", transactionTimeoutSeconds=").append(transactionTimeoutSeconds);
        sb.append(", xid=").append(xid);
        sb.append('}');
        return sb.toString();
    }
}
