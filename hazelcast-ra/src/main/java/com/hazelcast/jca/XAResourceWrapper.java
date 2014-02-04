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

    public XAResourceWrapper(ManagedConnectionImpl managedConnectionImpl) {
        this.managedConnection = managedConnectionImpl;
    }

    //XAResource --START
    @Override
    public void start(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA start: " + xid);

        switch (flags){
            case TMNOFLAGS:
                validateInner(false);
                setInner();
                inner.start(xid, flags);
                break;
            case TMRESUME:
            case TMJOIN:
                validateInner(true);
                inner.start(xid, flags);
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }

    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        managedConnection.log(Level.FINEST, "XA end: " + xid + ", " + flags);
        validateInner(true);
        inner.end(xid, flags);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA prepare: " + xid);
        validateInner(true);
        return inner.prepare(xid);
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        managedConnection.log(Level.FINEST, "XA commit: " + xid);
        validateInner(true);
        inner.commit(xid, onePhase);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        managedConnection.log(Level.FINEST, "XA rollback: " + xid);
        validateInner(true);
        inner.rollback(xid);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new XAException(XAException.XAER_PROTO);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (inner == null) {
            setInner();
        }
        return inner.isSameRM(xaResource);
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
        this.transactionTimeoutSeconds=seconds;
        return false;
    }

    private final void validateInner(boolean shouldExist) throws XAException {
        if (shouldExist && inner == null) {

        } else if (!shouldExist && inner != null) {
            throw new XAException(XAException.XAER_DUPID);
        }
    }

    private void setInner() throws XAException {
        final TransactionContext transactionContext = HazelcastTransactionImpl.createTransaction(this.getTransactionTimeout(), managedConnection.getHazelcastInstance());
        this.managedConnection.getTx().setTxContext(transactionContext);
        inner = transactionContext.getXaResource();
    }

    public String toString() {
        if (inner == null) {
            try {
                setInner();
                return inner.toString();
            } catch (XAException e) {
                e.printStackTrace();
            }
        }
        return super.toString();
    }
}
