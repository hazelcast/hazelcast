/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ThreadContext;

/**
 * Early version of XA support for Hazelcast.
 * This is still <b>BETA</b> code and should not be used in production code!
 */
public class XAResourceImpl implements XAResource {
/*
 * start
 * end
 * prepare
 * commit
 */
	private ManagedConnectionImpl managedConnection;
	private int transactionTimeout;
	private final static Map<Xid, ThreadContext> transactionCache = new ConcurrentHashMap<Xid, ThreadContext>();

	public XAResourceImpl(ManagedConnectionImpl managedConnectionImpl) {
		this.managedConnection = managedConnectionImpl;
	}
	
	public void start(Xid xid, int arg1) throws XAException {
		managedConnection.log(Level.FINEST, "XA start: " + xid);
		Transaction tx = managedConnection.getHazelcastInstance().getTransaction();
		if (Transaction.TXN_STATUS_ACTIVE != tx.getStatus()) {
			tx.setTransactionTimeout(transactionTimeout * 1000);
			tx.begin();
		}
		transactionCache.put(xid, ThreadContext.get());
		// No tx start here - already started
	}

	public void end(Xid xvid, int arg1) throws XAException {
		// Nothing to do
		managedConnection.log(Level.FINEST, "XA end: " + xvid + ", " + arg1);
	}
	
	public int prepare(Xid xid) throws XAException {
		managedConnection.log(Level.FINEST, "XA prepare: " + xid);
		//Everything is in memory, all modified elements are already locked by Hazelcast itself
		return XA_OK;
	}

	/**
	 * Used in {@link XAResourceImpl#doInRestoredThreadContext(Xid, Action)}
	 */
	protected interface Action {
		void run(Transaction tx);
	}
	
	protected void doInRestoredThreadContext(Xid xid, Action action) {
		ThreadContext oldTc = ThreadContext.get();
		try {
			ThreadContext tc = transactionCache.remove(xid);
			if (tc != null) {
				ThreadContext.get().setCallContext(tc.getCallContext());
				ThreadContext.get().setCurrentFactory(tc.getCurrentFactory());
				action.run(tc.getTransaction());
			} else {
				action.run(null);
			}
		} finally {
			ThreadContext.get().setCallContext(oldTc.getCallContext());
			ThreadContext.get().setCurrentFactory(oldTc.getCurrentFactory());
		}
	}
	
	public void commit(final Xid xid, boolean onePhase) throws XAException {
		doInRestoredThreadContext(xid, new Action() {
			public void run(Transaction tx) {
				if (tx != null) {
					managedConnection.log(Level.FINEST, "XA commit: " + xid);
					tx.commit();
				} else {
					managedConnection.log(Level.WARNING, "XA commit: No active transaction anymore" + xid);
				}
			}
		});
		
	}
	
	public void rollback(final Xid xid) throws XAException {
		doInRestoredThreadContext(xid, new Action() {
			public void run(Transaction tx) {
				if (tx != null) {
					managedConnection.log(Level.FINEST, "XA rollback: " + xid);
					tx.rollback();
				} else {
					managedConnection.log(Level.WARNING, "XA rollback: No active transaction anymore" + xid);
				}
				transactionCache.remove(xid);
			}
		});
	}

	public Xid[] recover(int arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA recover: " + arg0);
		return new Xid[0];
	}
	
	public void forget(Xid xvid) throws XAException {
		managedConnection.log(Level.FINEST, "XA forget: " + xvid);
		transactionCache.remove(xvid);
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA isSameRM: " + arg0 );
		boolean retValue = (arg0 instanceof XAResourceImpl);
		managedConnection.log(Level.FINE, "this is " + (retValue?"":"not") + " the same RM as " + arg0);
		return retValue;
	}

	public boolean setTransactionTimeout(int seconds) {
		this.transactionTimeout = seconds;
		return true;
	}
	
	public int getTransactionTimeout() throws XAException {
		return this.transactionTimeout;
	}

}
