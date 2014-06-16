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

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.logging.Level;

/**
 * XA Resource implementation for Hazelcast.
 *
 * @author asimarslan
 */
public class XAResourceWrapper implements XAResource {

    private final ManagedConnectionImpl managedConnection;
    private int transactionTimeoutSeconds;
    private XAResource inner;
    private volatile boolean isStarted;

    public XAResourceWrapper(ManagedConnectionImpl managedConnectionImpl) {
        this.managedConnection = managedConnectionImpl;
    }

    //XAResource --START
    @Override
    public void start(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA start: " + xid);

        switch (flags){
            case TMNOFLAGS:
                setInner();
                break;
            case TMRESUME:
            case TMJOIN:
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }

        if (inner != null) {
            isStarted = true;
            inner.start(xid, flags);
        }

    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA end: " + xid + ", " + flags);
        validateInner();
        inner.end(xid, flags);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA prepare: " + xid);
        validateInner();
        return inner.prepare(xid);
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        managedConnection.log(Level.FINEST, "XA commit: " + xid);
        validateInner();
        inner.commit(xid, onePhase);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA rollback: " + xid);
        validateInner();
        inner.rollback(xid);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new XAException(XAException.XAER_PROTO);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (xaResource instanceof XAResourceWrapper ){
            final ManagedConnectionImpl otherManagedConnection = ((XAResourceWrapper) xaResource).managedConnection;
            final HazelcastInstance hazelcastInstance = managedConnection.getHazelcastInstance();
            final HazelcastInstance otherHazelcastInstance = otherManagedConnection.getHazelcastInstance();
            return hazelcastInstance != null && hazelcastInstance.equals(otherHazelcastInstance);
        }
        return false;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        if (inner == null) {
            setInner();
        }
        return inner.recover(flag);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return transactionTimeoutSeconds;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        if (!isStarted) {
            this.transactionTimeoutSeconds = seconds;
            return true;
        }
        return false;
    }

    private final void validateInner() throws XAException {
        if (inner == null) {
            throw new XAException(XAException.XAER_NOTA);
        }
    }

    private void setInner() throws XAException {
        final TransactionContext transactionContext = HazelcastTransactionImpl.createTransaction(this.getTransactionTimeout(), managedConnection.getHazelcastInstance());
        this.managedConnection.getTx().setTxContext(transactionContext);
        inner = transactionContext.getXaResource();
    }

}
