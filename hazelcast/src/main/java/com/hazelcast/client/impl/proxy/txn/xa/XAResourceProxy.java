/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy.txn.xa;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.XATransactionClearRemoteCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionCollectTransactionsCodec;
import com.hazelcast.client.impl.protocol.codec.XATransactionFinalizeCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.ClientTransactionManagerService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.XAResourceImpl;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client side XaResource implementation
 */
public class XAResourceProxy extends ClientProxy implements HazelcastXAResource {

    private static final int DEFAULT_TIMEOUT_SECONDS = (int) MILLISECONDS.toSeconds(TransactionOptions.DEFAULT_TIMEOUT_MILLIS);

    private final ConcurrentMap<Long, TransactionContext> threadContextMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Xid, List<TransactionContext>> xidContextMap = new ConcurrentHashMap<>();
    private final AtomicInteger timeoutInSeconds = new AtomicInteger(DEFAULT_TIMEOUT_SECONDS);

    public XAResourceProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.get(currentThreadId());
        switch (flags) {
            case TMNOFLAGS:
                List<TransactionContext> contexts = new CopyOnWriteArrayList<>();
                List<TransactionContext> currentContexts = xidContextMap.putIfAbsent(xid, contexts);
                if (currentContexts != null) {
                    throw new XAException("There is already TransactionContexts for the given xid: " + xid);
                }
                TransactionContext context = createTransactionContext(xid);
                contexts.add(context);
                threadContextMap.put(threadId, context);
                break;
            case TMRESUME:
            case TMJOIN:
                List<TransactionContext> contextList = xidContextMap.get(xid);
                if (contextList == null) {
                    throw new XAException("There is no TransactionContexts for the given xid: " + xid);
                }
                if (threadContext == null) {
                    threadContext = createTransactionContext(xid);
                    threadContextMap.put(threadId, threadContext);
                    contextList.add(threadContext);
                }
                break;
            default:
                throw new XAException("Unknown flag!" + flags);
        }
    }

    private TransactionContext createTransactionContext(Xid xid) {
        ClientTransactionManagerService transactionManager = getContext().getTransactionManager();
        TransactionContext context = transactionManager.newXATransactionContext(xid, timeoutInSeconds.get());
        getTransaction(context).begin();
        return context;
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.remove(threadId);
        ILogger logger = getContext().getLoggingService().getLogger(this.getClass());
        if (threadContext == null && logger.isFinestEnabled()) {
            logger.finest("There is no TransactionContext for the current thread: " + threadId);
        }
        List<TransactionContext> contexts = xidContextMap.get(xid);
        if (contexts == null && logger.isFinestEnabled()) {
            logger.finest("There is no TransactionContexts for the given xid: " + xid);
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.get(xid);
        if (contexts == null) {
            throw new XAException("There is no TransactionContexts for the given xid: " + xid);
        }
        for (TransactionContext context : contexts) {
            XATransactionProxy transaction = getTransaction(context);
            transaction.prepare();
        }
        return XA_OK;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null && onePhase) {
            throw new XAException("There is no TransactionContexts for the given xid: " + xid);
        }
        if (contexts == null) {
            finalizeTransactionRemotely(xid, true);
            return;
        }

        for (TransactionContext context : contexts) {
            XATransactionProxy transaction = getTransaction(context);
            transaction.commit(onePhase);
        }
        clearRemoteTransactions(xid);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null) {
            finalizeTransactionRemotely(xid, false);
            return;
        }
        for (TransactionContext context : contexts) {
            getTransaction(context).rollback();
        }
        clearRemoteTransactions(xid);
    }

    private void finalizeTransactionRemotely(Xid xid, boolean isCommit) {
        SerializableXID serializableXID = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(),
                xid.getBranchQualifier());
        Data xidData = toData(serializableXID);
        ClientMessage request = XATransactionFinalizeCodec.encodeRequest(serializableXID, isCommit);
        invoke(request, xidData);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null) {
            throw new XAException("No context with the given xid: " + xid);
        }
        clearRemoteTransactions(xid);
    }

    private void clearRemoteTransactions(Xid xid) {
        SerializableXID serializableXID = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(),
                xid.getBranchQualifier());
        Data xidData = toData(serializableXID);
        ClientMessage request = XATransactionClearRemoteCodec.encodeRequest(serializableXID);
        invoke(request, xidData);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (this == xaResource) {
            return true;
        }
        String otherClusterName = null;
        if (xaResource instanceof XAResourceProxy) {
            otherClusterName = ((XAResourceProxy) xaResource).getClusterName();
        }
        if (xaResource instanceof XAResourceImpl) {
            otherClusterName = ((XAResourceImpl) xaResource).getClusterName();
        }
        return getClusterName().equals(otherClusterName);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        ClientMessage request = XATransactionCollectTransactionsCodec.encodeRequest();
        ClientMessage response = invoke(request);
        Collection<Xid> list = XATransactionCollectTransactionsCodec.decodeResponse(response);
        return list.toArray(new Xid[0]);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return timeoutInSeconds.get();
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        timeoutInSeconds.set(seconds == 0 ? DEFAULT_TIMEOUT_SECONDS : seconds);
        return true;
    }

    @Nonnull
    @Override
    public TransactionContext getTransactionContext() {
        long threadId = Thread.currentThread().getId();
        TransactionContext transactionContext = threadContextMap.get(threadId);
        if (transactionContext == null) {
            throw new IllegalStateException("No TransactionContext associated with current thread: " + threadId);
        }
        return transactionContext;
    }

    private XATransactionProxy getTransaction(TransactionContext context) {
        return ((XATransactionContextProxy) context).getTransaction();
    }

    private long currentThreadId() {
        return Thread.currentThread().getId();
    }

    private String getClusterName() {
        ClientTransactionManagerService transactionManager = getContext().getTransactionManager();
        return transactionManager.getClusterName();
    }

    @Override
    public String toString() {
        return "HazelcastXaResource{" + getClusterName() + '}';
    }
}
