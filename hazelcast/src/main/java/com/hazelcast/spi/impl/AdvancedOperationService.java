package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.*;

import java.util.Collection;
import java.util.Map;

class AdvancedOperationService implements OperationServiceImpl {

    private final NodeEngineImpl nodeEngine;

    AdvancedOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        throw new UnsupportedOperationException();
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