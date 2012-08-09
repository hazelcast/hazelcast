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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.CallContext;
import com.hazelcast.impl.ThreadContext;

public class ManagedConnectionImpl extends JcaBase implements ManagedConnection, 
		javax.resource.cci.LocalTransaction, LocalTransaction {
	private static final AtomicInteger idGen = new AtomicInteger();
	private static final ConcurrentMap<CallContext, CallContext> predecessors = new ConcurrentHashMap<CallContext, CallContext>();
	
	private final XAResourceImpl xaResource;
	private final ManagedConnectionFactoryImpl factory;
	private final ConnectionRequestInfo cxRequestInfo;
	// Application server will always register at least one listener
	private final List<ConnectionEventListener> connectionEventListeners = new ArrayList<ConnectionEventListener>(1);
	private transient final int id;

	private Transaction tx;
	private String txThreadId;

	public ManagedConnectionImpl(ConnectionRequestInfo cxRequestInfo,
			ManagedConnectionFactoryImpl factory) {
		log(Level.FINEST, "ManagedConnectionImpl");
		xaResource = new XAResourceImpl(this);
		id = idGen.incrementAndGet();

		this.factory = factory;
		this.cxRequestInfo = cxRequestInfo;

		factory.logHzConnectionEvent(this, HzConnectionEvent.CREATE);
	}

	public void addConnectionEventListener(ConnectionEventListener listener) {
		log(Level.FINEST, "addConnectionEventListener: " + listener);
		connectionEventListeners.add(listener);
	}

	public void associateConnection(Object arg0) throws ResourceException {
		log(Level.FINEST, "associateConnection: " + arg0);
	}

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

			this.tx = factory.getResourceAdapter().getHazelcast().getTransaction();
			this.txThreadId = Thread.currentThread().toString();
		} else {
			log(Level.INFO, "Ignoring duplicate TX begin event");
		}
	}

	private void doBegin() {
		log(Level.FINEST, "begin");
		getHazelcastInstance().getTransaction().begin();
		fireConnectionEvent(ConnectionEvent.LOCAL_TRANSACTION_STARTED);
	}

	public void cleanup() throws ResourceException {
		log(Level.FINEST, "cleanup");
		factory.logHzConnectionEvent(this, HzConnectionEvent.CLEANUP);
	}

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

	public void destroy() throws ResourceException {
		log(Level.FINEST, "destroy");
		factory.logHzConnectionEvent(this, HzConnectionEvent.DESTROY);
	}

	void fireConnectionEvent(int event) {
		fireConnectionEvent(event, null);
	}

	void fireConnectionEvent(int event, Connection conn) {
		log(Level.FINEST, "fireConnectionEvevnt: " + event);

		ConnectionEvent connnectionEvent = new ConnectionEvent(this, event);
		connnectionEvent.setConnectionHandle(conn);

		for (ConnectionEventListener listener : connectionEventListeners) {
			switch (event) {
			case ConnectionEvent.LOCAL_TRANSACTION_STARTED:
				if (isDeliverStartedEvent()) {
					listener.localTransactionStarted(connnectionEvent);
				}
				break;
			case ConnectionEvent.LOCAL_TRANSACTION_COMMITTED:
				if (isDeliverCommitedEvent()) {
					listener.localTransactionCommitted(connnectionEvent);
				}
				break;
			case ConnectionEvent.LOCAL_TRANSACTION_ROLLEDBACK:
				if (isDeliverRolledback()) {
					listener.localTransactionRolledback(connnectionEvent);
				}
				break;
			case ConnectionEvent.CONNECTION_CLOSED:
				if (isDeliverClosed()) {
					listener.connectionClosed(connnectionEvent);
				}
				break;
			default:
				log(Level.WARNING, "Uknown event ignored: " + event);
			}
			;
		}
	}

	public Object getConnection(Subject subject,
			ConnectionRequestInfo connectionRequestInfo) {
		log(Level.FINEST, "getConnection: " + subject + ", "
				+ connectionRequestInfo);
		// must be new per JCA spec
		return new ConnectionImpl(this, subject);
	}

	public ConnectionRequestInfo getCxRequestInfo() {
		return cxRequestInfo;
	}

	protected HazelcastInstance getHazelcastInstance() {
		return getResourceAdapter().getHazelcast();
	}

	public LocalTransaction getLocalTransaction() {
		log(Level.FINEST, "getLocalTransaction");
		return this;
	}

	public ManagedConnectionMetaData getMetaData() {
		return new ManagedConnectionMetaData();
	}

	private ResourceAdapterImpl getResourceAdapter() {
		return factory.getResourceAdapter();
	}

	public XAResource getXAResource() throws ResourceException {
		log(Level.FINEST, "getXAResource");
		// must be the same per JCA spec
		return xaResource;
	}

	protected boolean isDeliverClosed() {
		return true;
	}

	protected boolean isDeliverCommitedEvent() {
		return true;
	}

	protected boolean isDeliverRolledback() {
		return true;
	}

	protected boolean isDeliverStartedEvent() {
		return false;
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
		log(Level.FINEST, "removeConnectionEventListener: " + listener);
		connectionEventListeners.remove(listener);
	}

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

	@Override
	public String toString() {
		return "hazelcast.ManagedConnectionImpl [" + id + "]";
	}
}
