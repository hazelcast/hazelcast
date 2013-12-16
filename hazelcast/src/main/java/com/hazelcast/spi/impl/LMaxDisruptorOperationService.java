package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LMaxDisruptorOperationService implements InternalOperationService {

    private final NodeEngineImpl nodeEngine;
    private final RingBuffer<Slot> ringBuffer;
    private final Disruptor<Slot> disruptor;

    public LMaxDisruptorOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        ExecutorService executorService = Executors.newCachedThreadPool();
        disruptor = new Disruptor<Slot>(FACTORY, 1024, executorService);
        disruptor.handleEventsWith(new SlotEventHandler());
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        // 1. Claim a slot in the buffer...
        long sequence = ringBuffer.next();

        // 2.  Fill the slot with data...
        Slot slot = ringBuffer.get(sequence);
        slot.op = op;
        op.setServiceName(serviceName);
        op.setPartitionId(partitionId);
        // 3.  Publish make the data available for consumption
        ringBuffer.publish(sequence);
        return op;
    }

    public static final EventFactory<Slot> FACTORY = new EventFactory<Slot>() {
        public Slot newInstance() {
            return new Slot();
        }
    };

    public static class Slot {

        private Operation op;
    }

    public class SlotEventHandler implements EventHandler<Slot> {

        public void onEvent(final Slot slot, final long sequence, final boolean endOfBatch) throws Exception {
            Operation op = slot.op;
            try {
                op.setNodeEngine(nodeEngine);
                op.beforeRun();
                op.run();

                final Object response = op.returnsResponse() ? op.getResponse() : null;
                op.afterRun();
                op.set(response, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void handleOperation(Packet packet) {

    }

    @Override
    public void onMemberLeft(MemberImpl member) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void notifyBackupCall(long callId) {

    }

    @Override
    public void notifyRemoteCall(long callId, Object response) {

    }

    @Override
    public boolean isCallTimedOut(Operation op) {
        return false;
    }

    @Override
    public int getResponseQueueSize() {
        return 0;
    }

    @Override
    public int getOperationExecutorQueueSize() {
        return 0;
    }

    @Override
    public int getRunningOperationsCount() {
        return 0;
    }

    @Override
    public int getRemoteOperationsCount() {
        return 0;
    }

    @Override
    public int getOperationThreadCount() {
        return 0;
    }

    @Override
    public long getExecutedOperationCount() {
        return 0;
    }

    @Override
    public void runOperation(Operation op) {

    }

    @Override
    public void executeOperation(Operation op) {

    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
        return null;
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        return null;
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        return null;
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory) throws Exception {
        return null;
    }

    @Override
    public Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory, Collection<Integer> partitions) throws Exception {
        return null;
    }

    @Override
    public Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory, Address target) throws Exception {
        return null;
    }

    @Override
    public boolean send(Operation op, int partitionId, int replicaIndex) {
        return false;
    }

    @Override
    public boolean send(Operation op, Address target) {
        return false;
    }

    @Override
    public boolean send(Operation op, Connection connection) {
        return false;
    }
}
