package com.hazelcast.jca;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.hazelcast.core.Transaction;

public class XAResourceImpl implements XAResource {

	private ManagedConnectionImpl managedConnection;
	private int transactionTimeout;
	private final Map<Xid, Transaction> transactionCache = new ConcurrentHashMap<Xid, Transaction>();

	public XAResourceImpl(ManagedConnectionImpl ippManagedConnectionImpl) {
		this.managedConnection = ippManagedConnectionImpl;
	}
	
	public void start(Xid xid, int arg1) throws XAException {
		managedConnection.log(Level.FINEST, "XA start: " + xid);
		Transaction tx = managedConnection.getHazelcastInstance().getTransaction();
		
		transactionCache.put(xid, tx);
		// No tx start here - already started
	}

	public int prepare(Xid xid) throws XAException {
		managedConnection.log(Level.FINEST, "XA prepare: " + xid);
		//If it is in memory, it will be fine to commit
		return XA_OK;
	}

	
	public void commit(Xid xid, boolean onePhase) throws XAException {
		managedConnection.log(Level.FINEST, "XA commit: " + xid);
		transactionCache.remove(xid).commit();
	}
	
	public void rollback(Xid xid) throws XAException {
		managedConnection.log(Level.FINEST, "XA rollback: " + xid);
		transactionCache.remove(xid).rollback();
	}

	public void end(Xid xvid, int arg1) throws XAException {
		managedConnection.log(Level.INFO, "XA end: Really Commit transaction " + xvid + ", " + arg1);
		transactionCache.remove(xvid);
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
		managedConnection.log(Level.INFO, "this is " + (retValue?"":"not") + " the same RM as " + arg0);
		return retValue;
	}

	public boolean setTransactionTimeout(int transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
		return true;
	}
	
	public int getTransactionTimeout() throws XAException {
		return this.transactionTimeout;
	}

}
