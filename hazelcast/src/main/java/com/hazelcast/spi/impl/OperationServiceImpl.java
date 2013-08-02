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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mdogan 12/14/12
 */
final class OperationServiceImpl implements OperationService {

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong(0);
    private final ConcurrentMap<Long, RemoteCall> remoteCalls;
    private final ExecutorService[] operationExecutors;
    private final ExecutorService defaultOperationExecutor;
    private final ExecutorService responseExecutor;
    private final long defaultCallTimeout;
    private final Map<RemoteCallKey, RemoteCallKey> executingCalls;
    private final ConcurrentMap<Long, Semaphore> backupCalls;
    private final int operationThreadCount;
    private final EntryTaskScheduler<Object, ScheduledBackup> backupScheduler;

    OperationServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class.getName());
        defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        final int coreSize = Runtime.getRuntime().availableProcessors();
        final boolean reallyMultiCore = coreSize >= 8;
        final int concurrencyLevel = reallyMultiCore ? coreSize * 4 : 16;
        remoteCalls = new ConcurrentHashMap<Long, RemoteCall>(1000, 0.75f, concurrencyLevel);
        final int opThreadCount = node.getGroupProperties().OPERATION_THREAD_COUNT.getInteger();
        operationThreadCount =  opThreadCount > 0 ? opThreadCount : coreSize * 2;
        operationExecutors = new ExecutorService[operationThreadCount];
        for (int i = 0; i < operationExecutors.length; i++) {
            operationExecutors[i] = Executors.newSingleThreadExecutor(new OperationThreadFactory(i));
        }
        defaultOperationExecutor = nodeEngine.getExecutionService().getExecutor(ExecutionService.OPERATION_EXECUTOR);
        responseExecutor = Executors.newSingleThreadExecutor(new SingleExecutorThreadFactory(node.threadGroup,
                node.getConfigClassLoader(), node.getThreadNamePrefix("response")));
        executingCalls = new ConcurrentHashMap<RemoteCallKey, RemoteCallKey>(1000, 0.75f, concurrencyLevel);
        backupCalls = new ConcurrentHashMap<Long, Semaphore>(1000, 0.75f, concurrencyLevel);
        backupScheduler = EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(),
                new ScheduledBackupProcessor(), false);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) throw new IllegalArgumentException("Partition id cannot be negative!");
        return new InvocationBuilder(nodeEngine, serviceName, op, partitionId);
    }

    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        if (target == null) throw new IllegalArgumentException("Target cannot be null!");
        return new InvocationBuilder(nodeEngine, serviceName, op, target);
    }

    @PrivateApi
    void handleOperation(final Packet packet) {
        try {
            final Executor executor;
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                executor = responseExecutor;
            } else {
                final int partitionId = packet.getPartitionId();
                executor = getExecutor(partitionId);
            }
            executor.execute(new RemoteOperationProcessor(packet));
        } catch (RejectedExecutionException e) {
            if (nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    private Executor getExecutor(int partitionId) {
        return partitionId > -1 ? operationExecutors[partitionId % operationThreadCount] : defaultOperationExecutor;
    }

    private int getPartitionIdForExecution(Operation op) {
        return op instanceof PartitionAwareOperation ? op.getPartitionId() : -1;
    }

    /**
     * Runs operation in calling thread.
     * @param op
     */
    public void runOperation(Operation op) {
        if (isAllowedToRunInCurrentThread(op)) {
            doRunOperation(op);
        } else {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> " + Thread.currentThread());
        }
    }

    boolean isAllowedToRunInCurrentThread(Operation op) {
        final int partitionId = getPartitionIdForExecution(op);
        if (partitionId > -1) {
            final Thread currentThread = Thread.currentThread();
            if (currentThread instanceof OperationThread) {
                int tid = ((OperationThread) currentThread).id;
                return partitionId % operationThreadCount == tid;
            }
            return false;
        }
        return true;
    }

    boolean isInvocationAllowedFromCurrentThread(Operation op) {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof OperationThread) {
            final int partitionId = getPartitionIdForExecution(op);
            if (partitionId > -1) {
                int tid = ((OperationThread) currentThread).id;
                return partitionId % operationThreadCount == tid;
            }
            return true;
        }
        return true;
    }

    /**
     * Executes operation in operation executor pool.
     *
     * @param op
     */
    public void executeOperation(final Operation op) {
        final int partitionId = getPartitionIdForExecution(op);
        getExecutor(partitionId).execute(new LocalOperationProcessor(op));
    }

    /**
     * Runs operation in calling thread.
     */
    private void doRunOperation(final Operation op) {
        RemoteCallKey callKey = null;
        try {
            if (isCallTimedOut(op)) {
                Object response = new CallTimeoutException("Call timed out for " + op.getClass().getName()
                        + ", call-time: " + op.getInvocationTime() + ", timeout: " + op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
                return;
            }
            callKey = beforeCallExecution(op);
            final int partitionId = op.getPartitionId();
            if (op instanceof PartitionAwareOperation) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException("Partition id cannot be negative! -> " + partitionId);
                }
                final PartitionView partitionView = nodeEngine.getPartitionService().getPartition(partitionId);
                if (partitionView == null) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                            op.getClass().getName(), op.getServiceName());
                }
                if (retryDuringMigration(op) && node.partitionService.isPartitionMigrating(partitionId)) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                        op.getClass().getName(), op.getServiceName());
                }
                final Address owner = partitionView.getReplicaAddress(op.getReplicaIndex());
                if (op.validatesTarget() && !node.getThisAddress().equals(owner)) {
                    throw new WrongTargetException(node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                            op.getClass().getName(), op.getServiceName());
                }
            }

            OperationAccessor.setStartTime(op, Clock.currentTimeMillis());
            op.beforeRun();
            if (op instanceof WaitSupport) {
                WaitSupport waitSupport = (WaitSupport) op;
                if (waitSupport.shouldWait()) {
                    nodeEngine.waitNotifyService.await(waitSupport);
                    return;
                }
            }
            op.run();

            final boolean returnsResponse = op.returnsResponse();
            Object response = null;
            if (op instanceof BackupAwareOperation) {
                final BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
                int syncBackupCount = 0;
                if (backupAwareOp.shouldBackup()) {
                    syncBackupCount = sendBackups(backupAwareOp);
                }
                if (returnsResponse) {
                    response = new Response(op.getResponse(), op.getCallId(), syncBackupCount);
                }
            }
            if (returnsResponse) {
                if (response == null) {
                    response = op.getResponse();
                }
                final ResponseHandler responseHandler = op.getResponseHandler();
                if (responseHandler == null) {
                    throw new IllegalStateException("ResponseHandler should not be null!");
                }
                responseHandler.sendResponse(response);
            }

            op.afterRun();
            if (op instanceof Notifier) {
                final Notifier notifier = (Notifier) op;
                if (notifier.shouldNotify()) {
                    nodeEngine.waitNotifyService.notify(notifier);
                }
            }
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            afterCallExecution(op, callKey);
        }
    }

    private static boolean retryDuringMigration(Operation op) {
        return !(op instanceof ReadonlyOperation || OperationAccessor.isMigrationOperation(op));
    }

    boolean isCallTimedOut(Operation op) {
        if (op.returnsResponse() && op.getCallId() != 0) {
            final long callTimeout = op.getCallTimeout();
            final long invocationTime = op.getInvocationTime();
            final long expireTime = invocationTime + callTimeout;
            if (expireTime > 0 && expireTime < Long.MAX_VALUE) {
                final long now = nodeEngine.getClusterTime();
                if (expireTime < now) {
                    return true;
                }
            }
        }
        return false;
    }

    private RemoteCallKey beforeCallExecution(Operation op) {
        RemoteCallKey callKey = null;
        if (op.getCallId() != 0 && op.returnsResponse()) {
            callKey = new RemoteCallKey(op.getCallerAddress(), op.getCallId());
            RemoteCallKey current;
            if ((current = executingCalls.put(callKey, callKey)) != null) {
                logger.severe("Duplicate Call record! -> " + callKey + " / " + current + " == " + op.getClass().getName());
            }
        }
        return callKey;
    }

    private void afterCallExecution(Operation op, RemoteCallKey callKey) {
        if (callKey != null && op.getCallId() != 0 && op.returnsResponse()) {
            if (executingCalls.remove(callKey) == null) {
                logger.severe("No Call record has been found: -> " + callKey + " == " + op.getClass().getName());
            }
        }
    }

    private int sendBackups(BackupAwareOperation backupAwareOp) throws Exception {
        final Operation op = (Operation) backupAwareOp;
        final boolean returnsResponse = op.returnsResponse();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        final int maxBackups = Math.min(partitionService.getMemberGroupsSize() - 1, PartitionView.MAX_BACKUP_COUNT);

        int syncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOp.getSyncBackupCount()) : 0;

        int asyncBackupCount = (backupAwareOp.getAsyncBackupCount() > 0 && maxBackups > syncBackupCount)
                ? Math.min(maxBackups - syncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;

        if (!returnsResponse || op.isAsync()) {
            asyncBackupCount += syncBackupCount;
            syncBackupCount = 0;
        }

        final int totalBackupCount = syncBackupCount + asyncBackupCount;
        if (totalBackupCount > 0) {
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(partitionId, totalBackupCount);
            final PartitionView partition = partitionService.getPartition(partitionId);
            for (int replicaIndex = 1; replicaIndex <= totalBackupCount; replicaIndex++) {
                final Operation backupOp = backupAwareOp.getBackupOperation();
                if (backupOp == null) {
                    throw new IllegalArgumentException("Backup operation should not be null!");
                }

                backupOp.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(serviceName);
                final Backup backup = new Backup(backupOp, op.getCallerAddress(), replicaVersions, replicaIndex <= syncBackupCount);
                backup.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(serviceName)
                        .setCallerUuid(nodeEngine.getLocalMember().getUuid());
                OperationAccessor.setCallId(backup, op.getCallId());

                final Address target = partition.getReplicaAddress(replicaIndex);
                if (target != null) {
                    if (target.equals(node.getThisAddress())) {
                        throw new IllegalStateException("Normally shouldn't happen! Owner node and backup node are the same! " + partition);
                    } else {
                        send(backup, target);
                    }
                } else {
                    scheduleBackup(op, backup, partitionId, replicaIndex);
                }
            }
        }
        return syncBackupCount;
    }

    private void scheduleBackup(Operation op, Backup backup, int partitionId, int replicaIndex) {
        final RemoteCallKey key = new RemoteCallKey(op.getCallerAddress(), op.getCallId());
        if (logger.isFinestEnabled()) {
            logger.finest( "Scheduling -> " + backup);
        }
        backupScheduler.schedule(500, key, new ScheduledBackup(backup, partitionId, replicaIndex));
    }

    private class ScheduledBackupProcessor implements ScheduledEntryProcessor<Object, ScheduledBackup> {

        public void process(EntryTaskScheduler<Object, ScheduledBackup> scheduler, Collection<ScheduledEntry<Object, ScheduledBackup>> scheduledEntries) {
            for (ScheduledEntry<Object, ScheduledBackup> entry : scheduledEntries) {
                final ScheduledBackup backup = entry.getValue();
                if (!backup.backup()) {
                    final int retries = backup.retries;
                    if (logger.isFinestEnabled()) {
                        logger.finest( "Re-scheduling[" + retries + "] -> " + backup);
                    }
                    scheduler.schedule(entry.getScheduledDelayMillis() * retries, entry.getKey(), backup);
                }
            }
        }
    }

    private class ScheduledBackup {
        final Backup backup;
        final int partitionId;
        final int replicaIndex;
        volatile int retries = 0;

        private ScheduledBackup(Backup backup, int partitionId, int replicaIndex) {
            this.backup = backup;
            this.partitionId = partitionId;
            this.replicaIndex = replicaIndex;
        }

        public boolean backup() {
            final PartitionService partitionService = nodeEngine.getPartitionService();
            final PartitionView partition = partitionService.getPartition(partitionId);
            final Address target = partition.getReplicaAddress(replicaIndex);
            if (target != null && !target.equals(node.getThisAddress())) {
                send(backup, target);
                return true;
            }
            return ++retries >= 10; // if retried 10 times, give-up!
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ScheduledBackup{");
            sb.append("backup=").append(backup);
            sb.append(", partitionId=").append(partitionId);
            sb.append(", replicaIndex=").append(replicaIndex);
            sb.append('}');
            return sb.toString();
        }
    }

    private void handleOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        op.logError(e);
        if (node.isActive() && op.returnsResponse() && op.getResponseHandler() != null) {
            try {
                op.getResponseHandler().sendResponse(e);
            } catch (Throwable t) {
                logger.warning("While sending op error...", t);
            }
        }
    }

    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                   Collection<Integer> partitions) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = new HashMap<Address, List<Integer>>(3);
        for (int partition : partitions) {
            Address owner = nodeEngine.getPartitionService().getPartitionOwner(partition);
            if (!memberPartitions.containsKey(owner)) {
                memberPartitions.put(owner, new ArrayList<Integer>());
            }
            memberPartitions.get(owner).add(partition);
        }
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory,
                                                         Address target) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = Collections.singletonMap(target,
                nodeEngine.getPartitionService().getMemberPartitions(target));
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    private Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                                    Map<Address, List<Integer>> memberPartitions) throws Exception {
        final Thread currentThread = Thread.currentThread();
        if (currentThread instanceof OperationThread) {
            throw new IllegalThreadStateException(currentThread + " cannot make invocation on multiple partitions!");
        }
        final Map<Address, Future> responses = new HashMap<Address, Future>(memberPartitions.size());
        for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            final Address address = mp.getKey();
            final List<Integer> partitions = mp.getValue();
            final PartitionIteratingOperation pi = new PartitionIteratingOperation(partitions, operationFactory);
            Invocation inv = createInvocationBuilder(serviceName, pi, address).setTryCount(10).setTryPauseMillis(300).build();
            Future future = inv.invoke();
            responses.put(address, future);
        }
        final Map<Integer, Object> partitionResults = new HashMap<Integer, Object>(nodeEngine.getPartitionService().getPartitionCount());
        for (Map.Entry<Address, Future> response : responses.entrySet()) {
            try {
                PartitionResponse result = (PartitionResponse) nodeEngine.toObject(response.getValue().get());
                partitionResults.putAll(result.asMap());
            } catch (Throwable t) {
                if (logger.isFinestEnabled()) {
                    logger.finest(t);
                } else {
                    logger.warning(t.getMessage());
                }
                List<Integer> partitions = memberPartitions.get(response.getKey());
                for (Integer partition : partitions) {
                    partitionResults.put(partition, t);
                }
            }
        }
        final List<Integer> failedPartitions = new LinkedList<Integer>();
        for (Map.Entry<Integer, Object> partitionResult : partitionResults.entrySet()) {
            int partitionId = partitionResult.getKey();
            Object result = partitionResult.getValue();
            if (result instanceof Throwable) {
                failedPartitions.add(partitionId);
            }
        }
        for (Integer failedPartition : failedPartitions) {
            Invocation inv = createInvocationBuilder(serviceName,
                    operationFactory.createOperation(), failedPartition).build();
            Future f = inv.invoke();
            partitionResults.put(failedPartition, f);
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = (Future) partitionResults.get(failedPartition);
            Object result = f.get();
            partitionResults.put(failedPartition, result);
        }
        return partitionResults;
    }

    public boolean send(final Operation op, final int partitionId, final int replicaIndex) {
        Address target = nodeEngine.getPartitionService().getPartition(partitionId).getReplicaAddress(replicaIndex);
        if (target == null) {
            logger.warning("No target available for partition: " + partitionId + " and replica: " + replicaIndex);
            return false;
        }
        return send(op, target);
    }

    public boolean send(final Operation op, final Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        } else {
            return send(op, node.getConnectionManager().getOrConnect(target));
        }
    }

    public boolean send(final Operation op, final Connection connection) {
        Data data = nodeEngine.toData(op);
        final int partitionId = getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        if (op instanceof ResponseOperation) {
            packet.setHeader(Packet.HEADER_RESPONSE);
        }
        return nodeEngine.send(packet, connection);
    }

    @PrivateApi
    long registerRemoteCall(RemoteCall call) {
        final long callId = newCallId();
        remoteCalls.put(callId, call);
        return callId;
    }

    @PrivateApi
    long newCallId() {
        final long callId = callIdGen.incrementAndGet();
        if (callId == 0) {
            return newCallId();
        }
        return callId;
    }

    // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
    @PrivateApi
    void notifyRemoteCall(long callId, Object response) {
        RemoteCall call = deregisterRemoteCall(callId);
        if (call != null) {
            call.offerResponse(response);
        } else {
            throw new HazelcastException("No call with id: " + callId + ", Response: " + response);
        }
    }

    @PrivateApi
    RemoteCall deregisterRemoteCall(long callId) {
        return remoteCalls.remove(callId);
    }

    @PrivateApi
    void notifyBackupCall(long callId) {
        final Semaphore lock = backupCalls.get(callId);
        if (lock == null) {
            logger.warning("No backup record found for call[" + callId + "]!");
        } else {
            lock.release();
        }
    }

    @PrivateApi
    boolean waitForBackups(long callId, int backupCount, long timeout, TimeUnit unit) throws InterruptedException {
        final Semaphore lock = backupCalls.get(callId);
        if (lock == null) {
            throw new IllegalStateException("No backup record found for call -> " + callId);
        }
        try {
            return backupCount == 0 || lock.tryAcquire(backupCount, timeout, unit);
        } finally {
            backupCalls.remove(callId);
        }
    }

    @PrivateApi
    void registerBackupCall(long callId) {
        final Semaphore current = backupCalls.put(callId, new Semaphore(0));
        if (current != null) {
            logger.warning( "There is already a record for call[" + callId + "]!");
        }
    }

    @PrivateApi
    void deregisterBackupCall(long callId) {
        backupCalls.remove(callId);
    }

    @PrivateApi
    long getDefaultCallTimeout() {
        return defaultCallTimeout;
    }

    @PrivateApi
    boolean isOperationExecuting(Address caller, long operationCallId) {
        return executingCalls.containsKey(new RemoteCallKey(caller, operationCallId));
    }

    void onMemberLeft(final MemberImpl member) {
        // postpone notifying calls since real response may arrive in the mean time.
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                final Iterator<RemoteCall> iter = remoteCalls.values().iterator();
                while (iter.hasNext()) {
                    final RemoteCall call = iter.next();
                    if (call.isCallTarget(member)) {
                        iter.remove();
                        call.offerResponse(new MemberLeftException(member));
                    }
                }
            }
        }, 1111, TimeUnit.MILLISECONDS);
    }

    void shutdown() {
        logger.finest( "Stopping operation threads...");
        for (ExecutorService executor : operationExecutors) {
            executor.shutdown();
        }
        responseExecutor.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        for (RemoteCall call : remoteCalls.values()) {
            call.offerResponse(response);
        }
        remoteCalls.clear();
        backupCalls.clear();
        backupScheduler.cancelAll();
        for (ExecutorService executor : operationExecutors) {
            try {
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private class LocalOperationProcessor implements Runnable {
        private final Operation op;

        private LocalOperationProcessor(Operation op) {
            this.op = op;
        }

        public void run() {
            doRunOperation(op);
        }
    }

    private class RemoteOperationProcessor implements Runnable {
        final Packet packet;

        public RemoteOperationProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Operation op = (Operation) nodeEngine.toObject(data);
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);
                if (op instanceof ResponseOperation) {
                    processResponse((ResponseOperation) op);
                } else {
                    ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);
                    if (!OperationAccessor.isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                        final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                                op.getClass().getName(), op.getServiceName());
                        handleOperationError(op, error);
                    } else {
                        doRunOperation(op);
                    }
                }
            } catch (Throwable e) {
                logger.severe(e);
            }
        }

        void processResponse(ResponseOperation response) {
            try {
                response.beforeRun();
                response.run();
                response.afterRun();
            } catch (Throwable e) {
                logger.severe("While processing response...", e);
            }
        }
    }

    private class OperationThreadFactory extends AbstractExecutorThreadFactory {

        final String threadName;
        final int threadId;

        public OperationThreadFactory(int threadId) {
            super(node.threadGroup, node.getConfigClassLoader());
            final String poolNamePrefix = node.getThreadPoolNamePrefix("operation");
            this.threadName = poolNamePrefix + threadId;
            this.threadId = threadId;
        }

        protected Thread createThread(Runnable r) {
            return new OperationThread(threadGroup, r, threadName, threadId);
        }
    }

    private class OperationThread extends Thread {

        final int id;

        public OperationThread(ThreadGroup threadGroup, Runnable target, String name, int id) {
            super(threadGroup, target, name);
            this.id = id;
        }

        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            }
        }
    }

    private class RemoteCallKey {
        private final long time = Clock.currentTimeMillis();
        private final Address caller;
        private final long callId;

        private RemoteCallKey(Address caller, long callId) {
            this.caller = caller;
            this.callId = callId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteCallKey callKey = (RemoteCallKey) o;
            if (callId != callKey.callId) return false;
            if (!caller.equals(callKey.caller)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = caller.hashCode();
            result = 31 * result + (int) (callId ^ (callId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RemoteCallKey");
            sb.append("{caller=").append(caller);
            sb.append(", callId=").append(callId);
            sb.append(", time=").append(time);
            sb.append('}');
            return sb.toString();
        }
    }
}
