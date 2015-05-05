/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.txn.proxy.xa;

import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientTransactionManagerService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.client.CollectXATransactionsRequest;
import com.hazelcast.transaction.client.FinalizeXATransactionRequest;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.XAResourceImpl;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Client side XaResource implementation
 */
public class XAResourceProxy extends ClientProxy implements HazelcastXAResource {

    private static final int DEFAULT_TIMEOUT = (int) TimeUnit.MILLISECONDS.toSeconds(TransactionOptions.DEFAULT_TIMEOUT_MILLIS);
    private final ConcurrentMap<Long, TransactionContext> threadContextMap = new ConcurrentHashMap<Long, TransactionContext>();
    private final ConcurrentMap<Xid, List<TransactionContext>> xidContextMap
            = new ConcurrentHashMap<Xid, List<TransactionContext>>();

    public XAResourceProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.get(currentThreadId());
        switch (flags) {
            case TMNOFLAGS:
                List<TransactionContext> contexts = new CopyOnWriteArrayList<TransactionContext>();
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
                throw new XAException("Unknown flag!!! " + flags);
        }
    }

    private TransactionContext createTransactionContext(Xid xid) {
        ClientContext clientContext = getContext();
        ClientTransactionManagerService transactionManager = clientContext.getTransactionManager();
        TransactionContext context = transactionManager.newXaTransactionContext(xid, DEFAULT_TIMEOUT);
        getTransaction(context).begin();
        return context;
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.remove(threadId);
        if (threadContext == null) {
            throw new XAException("There is no TransactionContext for the current thread: " + threadId);
        }
        switch (flags) {
            case TMFAIL:
                List<TransactionContext> contexts = xidContextMap.remove(xid);
                if (contexts != null) {
                    for (TransactionContext context : contexts) {
                        getTransaction(context).rollback();
                    }
                }
                break;
            case TMSUCCESS:
            case TMSUSPEND:
                break;
            default:
                throw new XAException("Unknown flag!!! " + flags);
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
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null) {
            finalizeTransactionRemotely(xid, true);
            return;
        }
        for (TransactionContext context : contexts) {
            getTransaction(context).rollback();
        }
    }

    private void finalizeTransactionRemotely(Xid xid, boolean isCommit) {
        SerializableXID serializableXID =
                new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        SerializationService serializationService = getContext().getSerializationService();
        Data xidData = serializationService.toData(serializableXID);
        FinalizeXATransactionRequest request = new FinalizeXATransactionRequest(xidData, isCommit);
        invoke(request, xidData);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null) {
            throw new XAException("No context with the given xid: " + xid);
        }
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (this == xaResource) {
            return true;
        }
        String otherGroupName = null;
        if (xaResource instanceof XAResourceProxy) {
            otherGroupName = ((XAResourceProxy) xaResource).getGroupName();
        }
        if (xaResource instanceof XAResourceImpl) {
            otherGroupName = ((XAResourceImpl) xaResource).getGroupName();
        }
        return getGroupName().equals(otherGroupName);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        SerializableCollection xidSet = invoke(new CollectXATransactionsRequest());
        SerializableXID[] xidArray = new SerializableXID[xidSet.size()];
        SerializationService serializationService = getContext().getSerializationService();
        int index = 0;
        for (Data xidData : xidSet) {
            xidArray[index++] = serializationService.toObject(xidData);
        }
        return xidArray;
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return DEFAULT_TIMEOUT;
    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    @Override
    public TransactionContext getTransactionContext() {
        long threadId = Thread.currentThread().getId();
        return threadContextMap.get(threadId);
    }

    private XATransactionProxy getTransaction(TransactionContext context) {
        return ((XATransactionContextProxy) context).getTransaction();
    }

    private long currentThreadId() {
        return Thread.currentThread().getId();
    }

    private String getGroupName() {
        ClientContext context = getContext();
        ClientTransactionManagerService transactionManager = context.getTransactionManager();
        return transactionManager.getGroupName();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastXaResource {").append(getGroupName()).append('}');
        return sb.toString();
    }

}
