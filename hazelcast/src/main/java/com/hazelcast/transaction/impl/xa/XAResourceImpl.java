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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.ExceptionUtil;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.transaction.impl.xa.XAService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Server side XaResource implementation
 */
public final class XAResourceImpl extends AbstractDistributedObject<XAService> implements HazelcastXAResource {

    private static final int DEFAULT_TIMEOUT_SECONDS = (int) MILLISECONDS.toSeconds(TransactionOptions.DEFAULT_TIMEOUT_MILLIS);
    private static final ILogger LOGGER = Logger.getLogger(XAResourceImpl.class);

    private final ConcurrentMap<Long, TransactionContext> threadContextMap = new ConcurrentHashMap<Long, TransactionContext>();
    private final ConcurrentMap<Xid, List<TransactionContext>> xidContextMap
            = new ConcurrentHashMap<Xid, List<TransactionContext>>();
    private final String groupName;
    private final AtomicInteger timeoutInSeconds = new AtomicInteger(DEFAULT_TIMEOUT_SECONDS);

    public XAResourceImpl(NodeEngine nodeEngine, XAService service) {
        super(nodeEngine, service);
        GroupConfig groupConfig = nodeEngine.getConfig().getGroupConfig();
        groupName = groupConfig.getName();
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
        XAService xaService = getService();
        TransactionContext context = xaService.newXATransactionContext(xid, timeoutInSeconds.get());
        getTransaction(context).begin();
        return context;
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.remove(threadId);
        if (threadContext == null && LOGGER.isFinestEnabled()) {
            LOGGER.finest("There is no TransactionContext for the current thread: " + threadId);
        }
        List<TransactionContext> contexts = xidContextMap.get(xid);
        if (contexts == null && LOGGER.isFinestEnabled()) {
            LOGGER.finest("There is no TransactionContexts for the given xid: " + xid);
        }
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.get(xid);
        if (contexts == null) {
            throw new XAException("There is no TransactionContexts for the given xid: " + xid);
        }
        for (TransactionContext context : contexts) {
            Transaction transaction = getTransaction(context);
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
            Transaction transaction = getTransaction(context);
            if (onePhase) {
                transaction.prepare();
            }
            transaction.commit();
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

    private void finalizeTransactionRemotely(Xid xid, boolean isCommit) throws XAException {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();
        SerializableXID serializableXID =
                new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        Data xidData = nodeEngine.toData(serializableXID);
        int partitionId = partitionService.getPartitionId(xidData);
        FinalizeRemoteTransactionOperation operation = new FinalizeRemoteTransactionOperation(xidData, isCommit);
        InternalCompletableFuture<Integer> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
        Integer errorCode;
        try {
            errorCode = future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (errorCode != null) {
            throw new XAException(errorCode);
        }
    }

    private void clearRemoteTransactions(Xid xid) {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();
        SerializableXID serializableXID =
                new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        Data xidData = nodeEngine.toData(serializableXID);
        int partitionId = partitionService.getPartitionId(xidData);
        ClearRemoteTransactionOperation operation = new ClearRemoteTransactionOperation(xidData);
        operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        List<TransactionContext> contexts = xidContextMap.remove(xid);
        if (contexts == null) {
            throw new XAException("No context with the given xid: " + xid);
        }
        clearRemoteTransactions(xid);
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        if (this == xaResource) {
            return true;
        }
        if (xaResource instanceof XAResourceImpl) {
            XAResourceImpl otherXaResource = (XAResourceImpl) xaResource;
            return groupName.equals(otherXaResource.groupName);
        }
        return xaResource.isSameRM(this);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        NodeEngine nodeEngine = getNodeEngine();
        XAService xaService = getService();
        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        Collection<MemberImpl> memberList = clusterService.getMemberList();
        List<InternalCompletableFuture<SerializableCollection>> futureList
                = new ArrayList<InternalCompletableFuture<SerializableCollection>>();
        for (MemberImpl member : memberList) {
            if (member.localMember()) {
                continue;
            }
            CollectRemoteTransactionsOperation op = new CollectRemoteTransactionsOperation();
            Address address = member.getAddress();
            InternalCompletableFuture<SerializableCollection> future = operationService.invokeOnTarget(SERVICE_NAME, op, address);
            futureList.add(future);
        }
        HashSet<SerializableXID> xids = new HashSet<SerializableXID>();
        xids.addAll(xaService.getPreparedXids());
        for (InternalCompletableFuture<SerializableCollection> future : futureList) {
            SerializableCollection xidSet = future.getSafely();
            for (Data xidData : xidSet) {
                SerializableXID xid = nodeEngine.toObject(xidData);
                xids.add(xid);
            }
        }
        return xids.toArray(new SerializableXID[xids.size()]);
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

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public TransactionContext getTransactionContext() {
        long threadId = Thread.currentThread().getId();
        TransactionContext transactionContext = threadContextMap.get(threadId);
        if (transactionContext == null) {
            throw new IllegalStateException("No TransactionContext associated with current thread :" + threadId);
        }
        return transactionContext;
    }

    public String getGroupName() {
        return groupName;
    }

    private Transaction getTransaction(TransactionContext context) {
        return ((XATransactionContextImpl) context).getTransaction();
    }

    private long currentThreadId() {
        return Thread.currentThread().getId();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("HazelcastXaResource {").append(groupName).append('}');
        return sb.toString();
    }
}
