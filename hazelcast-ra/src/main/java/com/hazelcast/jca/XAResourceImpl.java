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

import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionAccessor;
import com.hazelcast.util.UuidUtil;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * XA Resource implementation for Hazelcast.
 *
 * @author asimarslan
 */
public class XAResourceImpl implements XAResource {

    private final ManagedConnectionImpl managedConnection;
    private int transactionTimeoutSeconds;

    private final static Map<Xid, Transaction> transactionCache = new ConcurrentHashMap<Xid, Transaction>();


    public XAResourceImpl(ManagedConnectionImpl managedConnectionImpl) {
        this.managedConnection = managedConnectionImpl;
    }

    //XAResource --START
    @Override
    public void start(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA start: " + xid);

        validateXID(xid);

        switch (flags){
            case TMNOFLAGS:
                if(getTransaction(xid) != null){
                    throw new XAException(XAException.XAER_DUPID);
                }

                final TransactionContext transactionContext = HazelcastTransactionImpl.createTransaction(this.getTransactionTimeout(), managedConnection.getHazelcastInstance());
                //setting related managedconnection transactionContext
                this.managedConnection.getTx().setTxContext(transactionContext);

                final Transaction tx = TransactionAccessor.getTransaction(transactionContext);
                try {
                    tx.begin();
                    putTransaction(xid, tx);
                } catch (IllegalStateException e) {
                    throw new XAException(XAException.XAER_INVAL);
                }
                break;
            case TMRESUME:
            case TMJOIN:
                validateXID(xid);
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA end: " + xid + ", " + flags);

        validateXID(xid);

        validateTx(xid,Transaction.State.ACTIVE);

        switch (flags) {
            case XAResource.TMSUCCESS:
                //successfully end.
                break;
            case XAResource.TMFAIL:
                final Transaction tx = getTransaction(xid);
                try {
                    tx.rollback();
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
    public int prepare(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA prepare: " + xid);

        validateXID(xid);

        validateTx(xid,Transaction.State.ACTIVE);

        final Transaction tx = getTransaction(xid);

        try {
            tx.prepare();
        } catch (TransactionException e) {
            managedConnection.log(Level.FINEST, e.getMessage());
            throw new XAException(XAException.XAER_RMERR);
        }
        return XAResource.XA_OK;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        managedConnection.log(Level.FINEST, "XA commit: " + xid);
        validateXID(xid);

        final Transaction tx = getTransaction(xid);

        if(onePhase){
            validateTx(xid,Transaction.State.ACTIVE);
            tx.prepare();
        }

        validateTx(xid,Transaction.State.PREPARED);

        try {
            tx.commit();
        } catch (TransactionException e) {
            managedConnection.log(Level.FINEST, e.getMessage());
            throw new XAException(XAException.XAER_RMERR);
        }
        removeTransaction(xid);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA rollback: " + xid);
        validateXID(xid);

        final Transaction tx = getTransaction(xid);

        try {
            tx.rollback();
        } catch (TransactionException e) {
            managedConnection.log(Level.FINEST, e.getMessage());
            throw new XAException(XAException.XAER_RMERR);
        }
        removeTransaction(xid);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new XAException(XAException.XAER_PROTO);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        return xaResource == this;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return new Xid[0];
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return transactionTimeoutSeconds;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        this.transactionTimeoutSeconds=seconds;
        return false;
    }

    //XAResource --END

    private Transaction getTransaction(Xid xid){
        return transactionCache.get(xid);
    }

    private void putTransaction(Xid xid,Transaction tx){
        transactionCache.put(xid,tx);
    }

    private void removeTransaction(Xid xid){
        transactionCache.remove(xid);
    }

    private void validateXID(Xid xid) throws XAException {
        if (xid == null){
            throw new XAException(XAException.XAER_INVAL);
        }
    }

    private void validateTx(Xid xid,Transaction.State state) throws XAException {
        final Transaction tx = getTransaction(xid);
        if(tx == null) {
            switch (state){
                case ACTIVE:
                    if(tx.getState() != Transaction.State.ACTIVE){
                        throw new XAException(XAException.XAER_NOTA);
                    }
                    break;
                case PREPARED:
                    if(tx.getState() != Transaction.State.PREPARED){
                        throw new XAException(XAException.XAER_INVAL);
                    }
                    break;
            }
        }
    }
}
