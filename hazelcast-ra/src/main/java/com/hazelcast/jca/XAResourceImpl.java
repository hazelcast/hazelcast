package com.hazelcast.jca;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.TransactionImpl;

public class XAResourceImpl implements XAResource {

	private ManagedConnectionImpl managedConnection;
	private int transactionTimeout;
	private boolean dirty;
	private final Map<Xid, Transaction> transactionCache = new ConcurrentHashMap<Xid, Transaction>();

	public XAResourceImpl(ManagedConnectionImpl managedConnection) {
		this.managedConnection = managedConnection;
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		managedConnection.log(Level.FINEST, "XA commit: " + arg0 + ", " + arg1);
		//delay commit to end - whatever is in memory won't break in the meantime
	}

	public void end(Xid arg0, int arg1) throws XAException {
		if (!dirty) {
			managedConnection.log(Level.INFO, "XA end: Really Commit transaction " + arg0 + ", " + arg1);
			getCurrentTransaction(arg0).commit();
		} else {
			managedConnection.log(Level.FINEST, "XA end: " + arg0 + ", " + arg1);
			//transaction already rolled back
		}
		dirty = false;
	}

	public void forget(Xid arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA forget: " + arg0);
		getCurrentTransaction(arg0).rollback();
		dirty = true;
	}

	public int getTransactionTimeout() throws XAException {
		return this.transactionTimeout;
	}

	public boolean isSameRM(XAResource arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA isSameRM: " + arg0 );
		boolean retValue = (arg0 instanceof XAResourceImpl);
		managedConnection.log(Level.INFO, "this is " + (retValue?"":"not") + " the same RM as " + arg0);
		return retValue;
	}

	private Transaction getCurrentTransaction(Xid arg0) {
		return transactionCache.get(arg0);
	}

	public int prepare(Xid arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA prepare: " + arg0);
		return XA_OK;
	}

	public Xid[] recover(int arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA recover: " + arg0);
		return new Xid[0];
	}

	public void rollback(Xid arg0) throws XAException {
		managedConnection.log(Level.FINEST, "XA rollback: " + arg0);
		forget(arg0);
	}

	public boolean setTransactionTimeout(int transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
		return true;
	}

	public void start(Xid arg0, int arg1) throws XAException {
		managedConnection.log(Level.FINEST, "XA start: " + arg0);
		Transaction tx = managedConnection.getHazelcastInstance().getTransaction();
		
		transactionCache.put(arg0, tx);
		
		tx.begin();
	}

}
