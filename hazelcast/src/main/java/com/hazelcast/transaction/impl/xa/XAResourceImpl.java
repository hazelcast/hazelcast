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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.AbstractDistributedObject;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.xa.operations.ClearRemoteTransactionOperation;
import com.hazelcast.transaction.impl.xa.operations.CollectRemoteTransactionsOperation;
import com.hazelcast.transaction.impl.xa.operations.FinalizeRemoteTransactionOperation;
import com.hazelcast.internal.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.transaction.impl.xa.XAService.SERVICE_NAME;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Server side XaResource implementation
 */
public final class XAResourceImpl extends AbstractDistributedObject<XAService> implements HazelcastXAResource {

    private static final int DEFAULT_TIMEOUT_SECONDS = (int) MILLISECONDS.toSeconds(TransactionOptions.DEFAULT_TIMEOUT_MILLIS);

    private final ConcurrentMap<Long, TransactionContext> threadContextMap = new ConcurrentHashMap<Long, TransactionContext>();
    private final ConcurrentMap<Xid, List<TransactionContext>> xidContextMap
            = new ConcurrentHashMap<Xid, List<TransactionContext>>();
    private final String clusterName;
    private final AtomicInteger timeoutInSeconds = new AtomicInteger(DEFAULT_TIMEOUT_SECONDS);
    private final ILogger logger;

    public XAResourceImpl(NodeEngine nodeEngine, XAService service) {
        super(nodeEngine, service);
        clusterName = nodeEngine.getConfig().getClusterName();
        logger = nodeEngine.getLogger(getClass());
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
                throw new XAException("Unknown flag! " + flags);
        }
    }

    private TransactionContext createTransactionContext(Xid xid) {
        XAService xaService = getService();
        TransactionContext context = xaService.newXATransactionContext(xid, null, timeoutInSeconds.get(), false);
        getTransaction(context).begin();
        return context;
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        long threadId = currentThreadId();
        TransactionContext threadContext = threadContextMap.remove(threadId);
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
        IPartitionService partitionService = nodeEngine.getPartitionService();
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
        IPartitionService partitionService = nodeEngine.getPartitionService();
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
            return clusterName.equals(otherXaResource.clusterName);
        }
        return xaResource.isSameRM(this);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        NodeEngine nodeEngine = getNodeEngine();
        XAService xaService = getService();
        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        Collection<Member> memberList = clusterService.getMembers();
        List<Future<SerializableList>> futureList = new ArrayList<Future<SerializableList>>();
        for (Member member : memberList) {
            if (member.localMember()) {
                continue;
            }
            CollectRemoteTransactionsOperation op = new CollectRemoteTransactionsOperation();
            Address address = member.getAddress();
            InternalCompletableFuture<SerializableList> future = operationService.invokeOnTarget(SERVICE_NAME, op, address);
            futureList.add(future);
        }
        Set<SerializableXID> xids = new HashSet<SerializableXID>(xaService.getPreparedXids());

        for (Future<SerializableList> future : futureList) {
            try {
                SerializableList xidSet = future.get();
                for (Data xidData : xidSet) {
                    SerializableXID xid = nodeEngine.toObject(xidData);
                    xids.add(xid);
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw new XAException(XAException.XAER_RMERR);
            } catch (MemberLeftException e) {
                logger.warning("Member left while recovering", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof HazelcastInstanceNotActiveException || cause instanceof TargetNotMemberException) {
                    logger.warning("Member left while recovering", e);
                } else {
                    throw new XAException(XAException.XAER_RMERR);
                }
            }
        }
        return xids.toArray(new SerializableXID[0]);
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

    public String getClusterName() {
        return clusterName;
    }

    private Transaction getTransaction(TransactionContext context) {
        return ((XATransactionContextImpl) context).getTransaction();
    }

    private long currentThreadId() {
        return Thread.currentThread().getId();
    }

    @Override
    public String toString() {
        return "HazelcastXaResource {" + clusterName + '}';
    }
}
