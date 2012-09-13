package com.hazelcast.jca;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.CallContext;
import com.hazelcast.impl.ThreadContext;

/**
 * Mapping class between Hazelcast's Transaction the the JCA-spec Transaction interfaces.
 * This class is aware of suspended transactions (by the container) and
 * transaction cleanup by different threads (as done by some containers).
 * 
 * All in all the JCA/tread context is created/restored in the method implementations of
 * {@link HazelcastTransaction} - The actual hazelcast transaction calls are in the doXXX methods
 * @see #doBegin()
 * @see #doCommit()
 * @see #doRollback()
 */
public class HazelcastTransactionImpl extends JcaBase implements HazelcastTransaction {
	/** List of former transaction used during transaction restore */
	private static final ConcurrentMap<CallContext, CallContext> predecessors = new ConcurrentHashMap<CallContext, CallContext>();
	/** access to the creator of this {@link #connection} */
	private final ManagedConnectionFactoryImpl factory;
	/** access to the creator of this transaction */
	private final ManagedConnectionImpl connection;
	/** The id of this transaction */
	private String txThreadId;
	/** The hazelcast transaction itself */
	private Transaction tx;
	
	public HazelcastTransactionImpl(ManagedConnectionFactoryImpl factory, ManagedConnectionImpl connection) {
		this.setLogWriter(factory.getLogWriter());

		this.factory = factory;
		this.connection = connection;
	}

	/** Delegates the hazelcast instance access to the @{link #connection} 
	 * @see ManagedConnectionImpl#getHazelcastInstance() 
	 */
	private HazelcastInstance getHazelcastInstance() {
		return connection.getHazelcastInstance();
	}

	/** Delegates the connection event propagation to the @{link #connection} 
	 * @see ManagedConnectionImpl#fireConnectionEvent(int) 
	 */
	private void fireConnectionEvent(int event) {
		connection.fireConnectionEvent(event);
	}

	/* (non-Javadoc)
	 * @see javax.resource.cci.LocalTransaction#begin()
	 */
	public void begin() throws ResourceException {
		if (null == tx) {
			factory.logHzConnectionEvent(this, HzConnectionEvent.TX_START);

			CallContext callContext = ThreadContext.get().getCallContext();
			Transaction tx = callContext.getTransaction();
			if ((null != tx) && (Transaction.TXN_STATUS_ACTIVE == tx.getStatus())) {
				log(Level.INFO, "Suspending outer TX");

				CallContext innerCallContext = new CallContext(callContext.getThreadId(), false);

				predecessors.put(innerCallContext, callContext);
				ThreadContext.get().setCallContext(innerCallContext);
			}

			doBegin();

			this.tx = getHazelcastInstance().getTransaction();
			this.txThreadId = Thread.currentThread().toString();
		} else {
			log(Level.INFO, "Ignoring duplicate TX begin event");
		}
	}

	/**
	 * Executes the begin on the  hazelcast transaction as the 
	 * JCA-context is restored by {@link #begin()} 
	 */
	private void doBegin() {
		log(Level.FINEST, "begin");
		getHazelcastInstance().getTransaction().begin();
		fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_STARTED);
	}

	/* (non-Javadoc)
	 * @see javax.resource.cci.LocalTransaction#commit()
	 */
	public void commit() throws ResourceException {
		factory.logHzConnectionEvent(this, HzConnectionEvent.TX_COMPLETE);

		CallContext callContext = ThreadContext.get().getCallContext();
		CallContext outerCallContext = predecessors.get(callContext);

		if (tx == callContext.getTransaction()) {
			doCommit();

			if (null != outerCallContext) {
				log(Level.INFO, "Restoring outer TX");
				ThreadContext.get().setCallContext(outerCallContext);
			}
		} else {
			log(Level.INFO, "txn.commit");
			tx.commit();
			fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);

			// TODO finalize TX on original thread
			callContext.finalizeTransaction();
			if (null != outerCallContext) {
				log(Level.INFO, "Restoring outer TX");
				callContext.setTransaction(outerCallContext.getTransaction());
			}

			String threadIx = Thread.currentThread().toString();
			log(Level.INFO, "Finalizing TX on thread " + threadIx
					+ " that was started on thread " + txThreadId);
		}

		if (null != outerCallContext) {
			predecessors.remove(callContext, outerCallContext);
		}

		this.tx = null;
		this.txThreadId = null;
	}

	/**
	 * Executes the commit on the  hazelcast transaction as the 
	 * JCA-context is restored by {@link #commit()} 
	 */
	private void doCommit() {
		log(Level.FINEST, "commit");
		Transaction transaction = ThreadContext.get().getTransaction();
		if (transaction != null) {
			transaction.commit();
			fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_COMMITTED);
		} else {
			log(Level.WARNING,
					"Missed transaction commit due to thread switch!");
		}
	}

	/* (non-Javadoc)
	 * @see javax.resource.cci.LocalTransaction#rollback()
	 */
	public void rollback() throws ResourceException {
		factory.logHzConnectionEvent(this, HzConnectionEvent.TX_COMPLETE);

		CallContext callContext = ThreadContext.get().getCallContext();
		CallContext outerCallContext = predecessors.get(callContext);

		if (tx == callContext.getTransaction()) {
			doRollback();

			if (null != outerCallContext) {
				log(Level.INFO, "Restoring outer TX");
				ThreadContext.get().setCallContext(outerCallContext);
			}
		} else {
			log(Level.INFO, "txn.rollback");
			tx.rollback();
			fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);

			// TODO finalize TX on original thread
			callContext.finalizeTransaction();
			if (null != outerCallContext) {
				log(Level.INFO, "Restoring outer TX");
				callContext.setTransaction(outerCallContext.getTransaction());
			}

			String threadIx = Thread.currentThread().toString();
			log(Level.INFO, "Finalizing TX on thread " + threadIx
					+ " that was started on thread " + txThreadId);
		}

		if (null != outerCallContext) {
			predecessors.remove(callContext, outerCallContext);
		}

		this.tx = null;
		this.txThreadId = null;
	}

	/**
	 * Executes the rollback on the  hazelcast transaction as the 
	 * JCA-context is restored by {@link #rollback()} 
	 */
	private void doRollback() {
		log(Level.FINEST, "rollback");
		Transaction transaction = ThreadContext.get().getTransaction();
		if (transaction != null) {
			transaction.rollback();
			fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK);
		} else {
			log(Level.WARNING,
					"Missed transaction rollback due to thread switch!");
		}
	}

}
