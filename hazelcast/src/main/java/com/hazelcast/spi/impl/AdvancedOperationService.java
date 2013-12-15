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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.*;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicBoolean;

class AdvancedOperationService implements OperationServiceImpl {

    private final NodeEngineImpl nodeEngine;
    private final PartitionScheduler[] partitionSchedulers;
    private final ForkJoinPool partitionForkJoinPool;
    private final Node node;

    AdvancedOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        int partitionCount = nodeEngine.getGroupProperties().PARTITION_COUNT.getInteger();
        partitionSchedulers = new PartitionScheduler[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitionSchedulers[partitionId] = new PartitionScheduler(partitionId);
        }

        final int opThreadCount = node.getGroupProperties().OPERATION_THREAD_COUNT.getInteger();
        final int coreSize = Runtime.getRuntime().availableProcessors();
        int operationThreadCount = opThreadCount > 0 ? opThreadCount : coreSize * 2;

        partitionForkJoinPool = new ForkJoinPool(
                operationThreadCount,
                new AdvancedPartitionThreadFactory(),
                new AdvancedUncaughtExceptionHandler(),
                true);
    }

    private final class PartitionScheduler implements Runnable{
        private final int partitionId;
        private final ConcurrentLinkedQueue workQueue = new ConcurrentLinkedQueue();
        private final AtomicBoolean scheduled = new AtomicBoolean();

        PartitionScheduler(int partitionId) {
            this.partitionId = partitionId;
        }

        public void schedule(Operation op){
            workQueue.offer(op);

            if(!scheduled.compareAndSet(false,true)){
                return;
            }

            partitionForkJoinPool.execute(this);
        }

        @Override
        public void run() {
            Operation op = (Operation) workQueue.poll();
            try{
                if(op!=null){
                    doRun(op);
                }
            }finally {
                scheduled.set(false);

                if(workQueue.isEmpty()){
                    return;
                }

                if(scheduled.get()){
                    return;
                }

                if(scheduled.compareAndSet(false,true)){
                    partitionForkJoinPool.execute(this);
                }
            }
        }

        private void doRun(Operation op) {
            try{
                op.setNodeEngine(nodeEngine);
                op.setPartitionId(partitionId);
                op.beforeRun();
                op.run();

                final Object response = op.returnsResponse()?op.getResponse():null;
                op.afterRun();
                op.set(response, false);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        PartitionScheduler scheduler = partitionSchedulers[partitionId];
        op.setServiceName(serviceName);
        scheduler.schedule(op);
        return op;
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        return new AdvancedInvocationBuilder(nodeEngine,serviceName,op,partitionId);
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return new AdvancedInvocationBuilder(nodeEngine,serviceName,op,target);
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory, Collection<Integer> partitions) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory, Address target) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean send(Operation op, int partitionId, int replicaIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean send(Operation op, Address target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean send(Operation op, Connection connection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleOperation(Packet packet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMemberLeft(MemberImpl member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        //no-op for the time being.
    }

    @Override
    public void notifyBackupCall(long callId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyRemoteCall(long callId, Object response) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCallTimedOut(Operation op) {
        return false;
    }

    @Override
    public int getResponseQueueSize() {
        //todo:
        return 0;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        //todo:
        return 0;
    }

    @Override
    public int getRunningOperationsCount() {
        //todo:
        return 0;
    }

    @Override
    public int getRemoteOperationsCount() {
        //todo:
        return 0;
    }

    @Override
    public int getOperationThreadCount() {
        //todo:
        return 0;
    }

    @Override
    public long getExecutedOperationCount() {
        //todo:
        return 0;
    }

    @Override
    public void runOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeOperation(Operation op) {
        throw new UnsupportedOperationException();
    }
}

class AdvancedPartitionThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory{
    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new AdvancedPartitionThread(pool);
    }
}

class AdvancedPartitionThread extends ForkJoinWorkerThread{
    AdvancedPartitionThread(ForkJoinPool pool) {
        super(pool);
    }
}

class AdvancedUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler{
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        e.printStackTrace();
    }
}

 class AdvancedInvocationBuilder extends AbstractInvocationBuilder{

     AdvancedInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
         super(nodeEngine, serviceName, op, partitionId);
     }

     AdvancedInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
         super(nodeEngine, serviceName, op, target);
     }

     @Override
     public InternalCompletableFuture invoke() {
        throw new UnsupportedOperationException();
     }
 }