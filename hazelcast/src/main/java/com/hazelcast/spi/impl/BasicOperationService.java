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
import com.hazelcast.core.PartitionAware;
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
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.AbstractExecutorThreadFactory;
import com.hazelcast.util.executor.ManagedExecutorService;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.util.scheduler.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the Basic InternalOperationService and depends on Java 6.
 *
 * All the classes that begin with 'Basic' are implementation detail that depend on the
 * {@link com.hazelcast.spi.impl.BasicOperationService}.
 *
 * <h1>System Operation</h1>
 * When a {@link com.hazelcast.spi.UrgentSystemOperation} is invoked on this OperationService, it will be executed with a
 * high urgency by making use of a urgent queue. So when the system is under load, and the operation queues are
 * filled, then system operations are executed before normal operation. The advantage is that when a system is under
 * pressure, it still is able to do things like recognizing new members in the cluster and moving partitions around.
 *
 * When a UrgentSystemOperation is send to a remote machine, it is wrapped in a {@link Packet} and the packet is marked as a
 * urgent packet. When this packet is received on the remove OperationService, the urgent flag is checked and if
 * needed, the operation is set on the urgent queue. So local and remote execution of System operations will obey
 * the urgency.
 *
 * @author mdogan 12/14/12
 * @see com.hazelcast.spi.impl.BasicInvocation
 * @see com.hazelcast.spi.impl.BasicPartitionInvocation
 * @see com.hazelcast.spi.impl.BasicTargetInvocation
 */
final class BasicOperationService implements InternalOperationService {

    private final static AtomicLong x = new AtomicLong();

    private final AtomicLong executedOperationsCount = new AtomicLong();

    final NodeEngineImpl nodeEngine;
    private final Node node;
    private final ILogger logger;
    private final AtomicLong callIdGen = new AtomicLong((x.incrementAndGet()*1000000));
     final ConcurrentMap<Long, BasicInvocation> localInvocations;
    private final ExecutorService[] operationExecutors;
    private final BlockingQueue[] operationExecutorQueues;
    private final ExecutorService defaultOperationExecutor;
    private final ConcurrentLinkedQueue defaultOperationUrgentQueue;
    private final ExecutorService responseExecutor;
    private final long defaultCallTimeout;
    private final Map<RemoteCallKey, RemoteCallKey> executingCalls;
    private final int operationThreadCount;
    private final EntryTaskScheduler<Object, ScheduledBackup> backupScheduler;
    private final BlockingQueue<Runnable> responseWorkQueue = new LinkedBlockingQueue<Runnable>();
    private final ConcurrentLinkedQueue[] operationExecutorUrgentQueues;
    private final ExecutionService executionService;

    BasicOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationService.class.getName());
        defaultCallTimeout = node.getGroupProperties().OPERATION_CALL_TIMEOUT_MILLIS.getLong();
        final int coreSize = Runtime.getRuntime().availableProcessors();
        final boolean reallyMultiCore = coreSize >= 8;
        final int concurrencyLevel = reallyMultiCore ? coreSize * 4 : 16;
        localInvocations = new ConcurrentHashMap<Long, BasicInvocation>(1000, 0.75f, concurrencyLevel);
        final int opThreadCount = node.getGroupProperties().OPERATION_THREAD_COUNT.getInteger();
        operationThreadCount =  opThreadCount > 0 ? opThreadCount : coreSize * 2;
        operationExecutors = new ExecutorService[operationThreadCount];
        operationExecutorQueues = new BlockingQueue[operationThreadCount];
        operationExecutorUrgentQueues = new ConcurrentLinkedQueue[operationThreadCount];
        for (int i = 0; i < operationExecutors.length; i++) {
            BlockingQueue<Runnable> operationExecutorQueue = new LinkedBlockingQueue<Runnable>();
            operationExecutorQueues[i] = operationExecutorQueue;

            ConcurrentLinkedQueue<Runnable> operationExecutorUrgentQueue = new ConcurrentLinkedQueue<Runnable>();
            operationExecutorUrgentQueues[i]=operationExecutorUrgentQueue;

            operationExecutors[i] = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    operationExecutorQueue,
                    new OperationThreadFactory(i));
        }

        executionService = nodeEngine.getExecutionService();
        defaultOperationUrgentQueue = new ConcurrentLinkedQueue<Runnable>();
        defaultOperationExecutor = executionService.register(ExecutionService.OPERATION_EXECUTOR,
                coreSize * 2, coreSize * 100000);

        executionService.register(ExecutionService.ASYNC_EXECUTOR, coreSize * 10, coreSize * 100000);

        responseExecutor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                responseWorkQueue,
                new SingleExecutorThreadFactory(node.threadGroup,
                        node.getConfigClassLoader(), node.getThreadNamePrefix("response")));

        executingCalls = new ConcurrentHashMap<RemoteCallKey, RemoteCallKey>(1000, 0.75f, concurrencyLevel);
        backupScheduler = EntryTaskSchedulerFactory.newScheduler(executionService.getScheduledExecutor(),
                new ScheduledBackupProcessor(), ScheduleType.SCHEDULE_IF_NEW);
    }

    //for testing
    int getRegisteredInvocationCount(){
        return localInvocations.size();
    }

    @Override
    public int getOperationThreadCount(){
        return operationThreadCount;
    }

    @Override
    public int getRunningOperationsCount(){
        return executingCalls.size();
    }

    @Override
    public long getExecutedOperationCount(){
        return executedOperationsCount.get();
    }

    @Override
    public int getRemoteOperationsCount(){
        return localInvocations.size();
    }

    @Override
    public int getResponseQueueSize() {
        return responseWorkQueue.size();
    }

    @Override
    public int getOperationExecutorQueueSize() {
        int size = 0;
        for(BlockingQueue q: operationExecutorQueues){
            size+=q.size();
        }
        return size;
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, final int partitionId) {
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition id cannot be negative!");
        }
        return new BasicInvocationBuilder(serviceName, op, partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null!");
        }
        return new BasicInvocationBuilder(serviceName, op, target);
    }

    @PrivateApi
    public void receive(final Packet packet) {
        try {
            if (packet.isHeaderSet(Packet.HEADER_RESPONSE)) {
                responseExecutor.execute(new ResponseProcessor(packet));
            } else {
                final int partitionId = packet.getPartitionId();
                final Executor executor = getExecutor(partitionId);
                if(packet.isUrgent()){
                    ConcurrentLinkedQueue<Runnable> urgentQueue = getUrgentQueue(partitionId);
                    urgentQueue.add(new RemoteOperationProcessor(packet));
                    executor.execute(new UrgentSystemOperationsProcessor());
                } else{
                    executor.execute(new RemoteOperationProcessor(packet));
                }
            }
        } catch (RejectedExecutionException e) {
            if (nodeEngine.isActive()) {
                throw e;
            }
        }
    }

    private Executor getExecutor(int partitionId) {
        return partitionId > -1 ? operationExecutors[partitionId % operationThreadCount] : defaultOperationExecutor;
    }

    private ConcurrentLinkedQueue<Runnable> getUrgentQueue(int partitionId) {
        return partitionId > -1 ? operationExecutorUrgentQueues[partitionId % operationThreadCount] : defaultOperationUrgentQueue;
    }

    private int getPartitionIdForExecution(Operation op) {
        return op instanceof PartitionAwareOperation ? op.getPartitionId() : -1;
    }

    /**
     * Runs operation in calling thread.
     * @param op
     */
    @Override
    public void runOperation(Operation op) {
        if (isAllowedToRunInCurrentThread(op)) {
            doRunOperation(op);
        } else {
            throw new IllegalThreadStateException("Operation: " + op + " cannot be run in current thread! -> " + Thread.currentThread());
        }
    }

    public boolean isOperationThread() {
        return Thread.currentThread() instanceof OperationThread;
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
    @Override
    public void executeOperation(final Operation op) {
        String executorName = op.getExecutorName();
        if (executorName == null) {
            final int partitionId = getPartitionIdForExecution(op);
            if (op instanceof UrgentSystemOperation) {
                getUrgentQueue(partitionId).offer(new LocalOperationProcessor(op));
                getExecutor(partitionId).execute(new UrgentSystemOperationsProcessor());
            } else {
                getExecutor(partitionId).execute(new LocalOperationProcessor(op));
            }
        } else {
            ManagedExecutorService executor = executionService.getExecutor(executorName);
            if(executor == null){
                throw new IllegalStateException("Could not found executor with name: "+executorName);
            }
            if(op instanceof PartitionAware){
                throw new IllegalStateException("PartitionAwareOperation "+op+" can't be executed on a custom executor with name: "+executorName);
            }
            if(op instanceof UrgentSystemOperation){
                throw new IllegalStateException("UrgentSystemOperation "+op+" can't be executed on a custom executor with name: "+executorName);
            }
            executor.execute(new LocalOperationProcessor(op));
        }
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
         return new BasicPartitionInvocation(this, serviceName, op, partitionId, InvocationBuilder.DEFAULT_REPLICA_INDEX,
                 InvocationBuilder.DEFAULT_TRY_COUNT, InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                 InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, null,InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        return new BasicTargetInvocation(this, serviceName, op, target, InvocationBuilder.DEFAULT_TRY_COUNT, InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS,
                InvocationBuilder.DEFAULT_CALL_TIMEOUT, null, null, InvocationBuilder.DEFAULT_DESERIALIZE_RESULT).invoke();
    }

    /**
     * Runs operation in calling thread.
     */
    private void doRunOperation(final Operation op) {
        executedOperationsCount.incrementAndGet();

        runSystemOperations();

        RemoteCallKey callKey = null;
        try {
            if (isCallTimedOut(op)) {
                Object response = new CallTimeoutException(op.getClass().getName(), op.getInvocationTime(), op.getCallTimeout());
                op.getResponseHandler().sendResponse(response);
                return;
            }
            callKey = beforeCallExecution(op);
            final int partitionId = op.getPartitionId();
            if (op instanceof PartitionAwareOperation) {
                if (partitionId < 0) {
                    throw new IllegalArgumentException("Partition id cannot be negative! -> " + partitionId);
                }
                final InternalPartition internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
                if (retryDuringMigration(op) && internalPartition.isMigrating()) {
                    throw new PartitionMigratingException(node.getThisAddress(), partitionId,
                        op.getClass().getName(), op.getServiceName());
                }
                final Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
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
                    response = new NormalResponse(op.getResponse(), op.getCallId(), syncBackupCount,op.isUrgent());
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

    private void runSystemOperations() {
        Thread thread = Thread.currentThread();
        ConcurrentLinkedQueue<Runnable> urgentQueue;
        if(thread instanceof  OperationThread){
             int id = ((OperationThread)thread).id;
             urgentQueue = operationExecutorUrgentQueues[id];
        } else{
             urgentQueue = defaultOperationUrgentQueue;
        }

        if(urgentQueue.isEmpty()){
            return;
        }

        for (; ; ) {
            Runnable task = urgentQueue.poll();
            if (task == null) {
                return;
            }

            task.run();
        }
    }

    private static boolean retryDuringMigration(Operation op) {
        return !(op instanceof ReadonlyOperation || OperationAccessor.isMigrationOperation(op));
    }

    @PrivateApi
    public boolean isCallTimedOut(Operation op) {
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
            callKey = new RemoteCallKey(op.getCallerAddress(), op.getCallerUuid(), op.getCallId());
            RemoteCallKey current;
            if ((current = executingCalls.put(callKey, callKey)) != null) {
                logger.warning("Duplicate Call record! -> " + callKey + " / " + current + " == " + op.getClass().getName());
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
        final int maxBackups = Math.min(partitionService.getMemberGroupsSize() - 1, InternalPartition.MAX_BACKUP_COUNT);

        int syncBackupCount = backupAwareOp.getSyncBackupCount() > 0
                ? Math.min(maxBackups, backupAwareOp.getSyncBackupCount()) : 0;

        int asyncBackupCount = (backupAwareOp.getAsyncBackupCount() > 0 && maxBackups > syncBackupCount)
                ? Math.min(maxBackups - syncBackupCount, backupAwareOp.getAsyncBackupCount()) : 0;

        if (!returnsResponse) {
            asyncBackupCount += syncBackupCount;
            syncBackupCount = 0;
        }

        final int totalBackupCount = syncBackupCount + asyncBackupCount;
        if (totalBackupCount > 0) {
            final String serviceName = op.getServiceName();
            final int partitionId = op.getPartitionId();
            final long[] replicaVersions = partitionService.incrementPartitionReplicaVersions(partitionId, totalBackupCount);
            final InternalPartition partition = partitionService.getPartition(partitionId);
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
        final RemoteCallKey key = new RemoteCallKey(op.getCallerAddress(), op.getCallerUuid(), op.getCallId());
        if (logger.isFinestEnabled()) {
            logger.finest( "Scheduling -> " + backup);
        }
        backupScheduler.schedule(500, key, new ScheduledBackup(backup, partitionId, replicaIndex));
    }

    private class ScheduledBackupProcessor implements ScheduledEntryProcessor<Object, ScheduledBackup> {

        @Override
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
            final InternalPartition partition = partitionService.getPartition(partitionId);
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
        ResponseHandler responseHandler = op.getResponseHandler();
        if (op.returnsResponse() && responseHandler != null) {
            try {
                if (node.isActive()) {
                    responseHandler.sendResponse(e);
                } else if (responseHandler.isLocal()) {
                    responseHandler.sendResponse(new HazelcastInstanceNotActiveException());
                }
            } catch (Throwable t) {
                logger.warning("While sending op error...", t);
            }
        }
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        final Map<Address, List<Integer>> memberPartitions = nodeEngine.getPartitionService().getMemberPartitionsMap();
        return invokeOnPartitions(serviceName, operationFactory, memberPartitions);
    }

    @Override
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
            Future future = createInvocationBuilder(serviceName, pi, address).setTryCount(10).setTryPauseMillis(300).invoke();
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
            Future f = createInvocationBuilder(serviceName,
                    operationFactory.createOperation(), failedPartition).invoke();
            partitionResults.put(failedPartition, f);
        }
        for (Integer failedPartition : failedPartitions) {
            Future f = (Future) partitionResults.get(failedPartition);
            Object result = f.get();
            partitionResults.put(failedPartition, result);
        }
        return partitionResults;
    }


    @Override
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

    @Override
    public boolean send(Response response, Address target) {
        if (target == null) {
            throw new IllegalArgumentException("Target is required!");
        }
        if (nodeEngine.getThisAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", response: " + response);
        }
        Data data = nodeEngine.toData(response);
        Packet packet = new Packet(data, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        packet.setHeader(Packet.HEADER_RESPONSE);
        if (response.isUrgent()) {
            packet.setHeader(Packet.HEADER_URGENT);
        }
        return nodeEngine.send(packet, node.getConnectionManager().getOrConnect(target));
    }

    private boolean send(final Operation op, final Connection connection) {
        Data data = nodeEngine.toData(op);
        final int partitionId = getPartitionIdForExecution(op);
        Packet packet = new Packet(data, partitionId, nodeEngine.getSerializationContext());
        packet.setHeader(Packet.HEADER_OP);
        if(op instanceof UrgentSystemOperation){
            packet.setHeader(Packet.HEADER_URGENT);
        }
        return nodeEngine.send(packet, connection);
    }

    @PrivateApi
    long registerInvocation(BasicInvocation invocation) {
        long callId = newCallId();
        localInvocations.put(callId, invocation);
        return callId;
    }

    //todo: simplify, just start counter at one.
    @PrivateApi
    long newCallId() {
        final long callId = callIdGen.incrementAndGet();
        if (callId == 0) {
            return newCallId();
        }
        return callId;
    }

    @PrivateApi
    BasicInvocation deregisterInvocation(long callId) {
        BasicInvocation invocation = localInvocations.remove(callId);
        //todo: remove logging from severe
        if(invocation == null){
            logger.severe("Deregistering non existing invocation");
        }
        return invocation;
    }

    @PrivateApi
    long getDefaultCallTimeout() {
        return defaultCallTimeout;
    }

    @PrivateApi
    boolean isOperationExecuting(Address callerAddress, String callerUuid, long operationCallId) {
        return executingCalls.containsKey(new RemoteCallKey(callerAddress, callerUuid, operationCallId));
    }

    @Override
    public void onMemberLeft(final MemberImpl member) {
        // postpone notifying calls since real response may arrive in the mean time.
        nodeEngine.getExecutionService().schedule(new Runnable() {
            public void run() {
                final Iterator<BasicInvocation> it = localInvocations.values().iterator();
                while (it.hasNext()) {
                    final BasicInvocation invocation = it.next();
                    if (invocation.isCallTarget(member)) {
                        it.remove();
                        invocation.invoke(new MemberLeftException(member));
                    }
                }
            }
        }, 1111, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        logger.finest( "Stopping operation threads...");
        for (ExecutorService executor : operationExecutors) {
            executor.shutdown();
        }
        responseExecutor.shutdown();
        for (BasicInvocation invocation : localInvocations.values()) {
            invocation.invoke(new HazelcastInstanceNotActiveException());
        }
        localInvocations.clear();
         backupScheduler.cancelAll();
        for (ExecutorService executor : operationExecutors) {
            try {
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Processes the System Operations. Normally they are going to be processed before normal execution of operations,
     * but if there is no work triggering a worker thread, then the system operation put in a urgent queue
     * are not going to be picked up by a worker thread.
     *
     * So when a system operation is send, also a UrgentSystemOperationsProcessor is send to the executor to make sure
     * that the operations are picked up. If the system operations already have been processed, then processor
     * doesn't do anything. So it can safely be send multiple times, without causing problems.
     */
    private class UrgentSystemOperationsProcessor implements Runnable{
        @Override
        public void run(){
            try {
                runSystemOperations();
            } catch (Throwable e) {
                logger.severe("While processing system operations...", e);
            }
        }
    }

    /**
     * Process the operation that has been send locally to this OperationService.
     */
    private class LocalOperationProcessor implements Runnable {
        private final Operation op;

        private LocalOperationProcessor(Operation op) {
            this.op = op;
        }

        @Override
        public void run() {
            doRunOperation(op);
        }
    }

    /**
     * Process an operation that has been send to this OperationService by a remote OperationService.
     */
    private class RemoteOperationProcessor implements Runnable {
        final Packet packet;

        public RemoteOperationProcessor(Packet packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            final Connection conn = packet.getConn();
            try {
                final Address caller = conn.getEndPoint();
                final Data data = packet.getData();
                final Object object = nodeEngine.toObject(data);
                final Operation op = (Operation) object;
                op.setNodeEngine(nodeEngine);
                OperationAccessor.setCallerAddress(op, caller);
                OperationAccessor.setConnection(op, conn);

                ResponseHandlerFactory.setRemoteResponseHandler(nodeEngine, op);
                if (!OperationAccessor.isJoinOperation(op) && node.clusterService.getMember(op.getCallerAddress()) == null) {
                    final Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                            op.getClass().getName(), op.getServiceName());
                    handleOperationError(op, error);
                } else {
                    String executorName = op.getExecutorName();
                    if (executorName == null) {
                        doRunOperation(op);
                    } else {
                        ManagedExecutorService executor = executionService.getExecutor(executorName);
                        if (executor == null) {
                            throw new IllegalStateException("Could not found executor with name: " + executorName);
                        }
                        executor.execute(new LocalOperationProcessor(op));
                    }
                }
            } catch (Throwable e) {
                logger.severe(e);
            }
        }
    }

    @Override
    public void notifyBackupCall(long callId) {
        try {
            final BasicInvocation invocation = localInvocations.get(callId);
            if (invocation != null) {
                invocation.signalOneBackupComplete();
            }
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    private class ResponseProcessor implements Runnable {
        final Packet packet;

        public ResponseProcessor(Packet packet) {
            this.packet = packet;
        }

        // TODO: @mm - operations those do not return response can cause memory leaks! Call->Invocation->Operation->Data
        private void notifyRemoteCall(NormalResponse response) {
            BasicInvocation invocation = localInvocations.get(response.getCallId());
            if (invocation == null) {
                throw new HazelcastException("No call for response:"+response);
            }

            invocation.invoke(response);
        }

        @Override
        public void run() {
            try {
                final Data data = packet.getData();
                final Response response = (Response)nodeEngine.toObject(data);

                if(response instanceof NormalResponse){
                    notifyRemoteCall((NormalResponse)response);
                }else if(response instanceof BackupResponse){
                    notifyBackupCall(response.getCallId());
                }else{
                    throw new IllegalStateException("Unrecognized response type: "+response);
                }
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

        @Override
        protected Thread createThread(Runnable r) {
            return new OperationThread(threadGroup, r, threadName, threadId);
        }
    }

    private static class OperationThread extends Thread {

        final int id;

        public OperationThread(ThreadGroup threadGroup, Runnable target, String name, int id) {
            super(threadGroup, target, name);
            this.id = id;
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            }
        }
    }

    private static class RemoteCallKey {
        private final long time = Clock.currentTimeMillis();
        private final Address callerAddress; // human readable caller
        private final String callerUuid;
        private final long callId;

        private RemoteCallKey(Address callerAddress, String callerUuid, long callId) {
            this.callerAddress = callerAddress;
            this.callerUuid = callerUuid;
            this.callId = callId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteCallKey callKey = (RemoteCallKey) o;
            if (callId != callKey.callId) return false;
            if (!callerUuid.equals(callKey.callerUuid)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = callerUuid.hashCode();
            result = 31 * result + (int) (callId ^ (callId >>> 32));
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("RemoteCallKey");
            sb.append("{callerAddress=").append(callerAddress);
            sb.append(", callerUuid=").append(callerUuid);
            sb.append(", callId=").append(callId);
            sb.append(", time=").append(time);
            sb.append('}');
            return sb.toString();
        }
    }

     private class BasicInvocationBuilder extends InvocationBuilder {

        public BasicInvocationBuilder(String serviceName, Operation op, int partitionId) {
            this(serviceName, op, partitionId, null);
        }

        public BasicInvocationBuilder(String serviceName, Operation op, Address target) {
            this(serviceName, op, -1, target);
        }

        private BasicInvocationBuilder(String serviceName, Operation op,
                                       int partitionId, Address target) {
            super(serviceName,op,partitionId,target);
        }

        @Override
        public InternalCompletableFuture invoke() {
            if (target == null) {
                return new BasicPartitionInvocation(BasicOperationService.this, serviceName, op, partitionId, replicaIndex,
                        tryCount, tryPauseMillis, callTimeout, callback, executorName,resultDeserialized).invoke();
            } else {
                return new BasicTargetInvocation(BasicOperationService.this, serviceName, op, target, tryCount, tryPauseMillis,
                        callTimeout, callback, executorName,resultDeserialized).invoke();
            }
        }
    }
}
