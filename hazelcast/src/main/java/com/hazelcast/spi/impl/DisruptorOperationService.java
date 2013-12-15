package com.hazelcast.spi.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class DisruptorOperationService implements OperationServiceImpl {

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final PartitionOperationScheduler[] schedulers;

    public DisruptorOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        int partitionCount = node.getGroupProperties().PARTITION_COUNT.getInteger();
        this.schedulers = new PartitionOperationScheduler[partitionCount];
        int ringbufferSize = 1024;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            schedulers[partitionId] = new PartitionOperationScheduler(partitionId,ringbufferSize);
        }
    }

    public class PartitionThreadScheduler{

    }

    /**
     * A Scheduler responsible for scheduling operations for a specific partitions.
     * The PartitionOperationScheduler will guarantee that at any given moment, at
     * most 1 thread will be active in that partition.
     *
     * todo:
     * - caller runs optimization
     * - improved thread assignment
     * - batching for the consumer
     * - system messages
     *
     * bad things:
     * - contention on the producersequenceref with concurrent producers
     *
     */
    public class PartitionOperationScheduler implements Runnable {
        private final int partitionId;

        private final Slot[] ringbuffer;

        private final AtomicLong producerSequenceRef = new AtomicLong(0);

        //we only have a single consumer
        private final AtomicLong consumerSequenceRef = new AtomicLong(0);

        private Executor executor = Executors.newFixedThreadPool(1);

        public PartitionOperationScheduler(final int partitionId, int ringBufferSize) {
            this.partitionId = partitionId;
            this.ringbuffer = new Slot[ringBufferSize];

            for (int k = 0; k < ringbuffer.length; k++) {
                ringbuffer[k] = new Slot();
            }
        }

        private boolean isEmpty() {
            return producerSequenceRef.get() == consumerSequenceRef.get();
        }

        public int toIndex(long sequence) {
            //todo: can be done more efficient by not using mod
            return (int) (sequence % ringbuffer.length);
        }

        public void schedule(Operation op) {
            //claim the slot
            long produceSequence = producerSequenceRef.incrementAndGet();

            //there is no happens before relation, but this will be introduced by the counter.write
            //and the counter.read by the consumer
            Slot slot = ringbuffer[toIndex(produceSequence)];
            slot.op = op;
            slot.commit(produceSequence);

            //now we need to make sure that it is scheduled
            if (produceSequence == consumerSequenceRef.get() + 1) {
                executor.execute(this);
            }
        }

        public Slot consume() {
            long consumerSequence = consumerSequenceRef.get();

            if (consumerSequence == producerSequenceRef.get()) {
                return null;
            }

            consumerSequence++;

            int slotIndex = toIndex(consumerSequence);

            Slot slot = ringbuffer[slotIndex];
            slot.awaitCommitted(consumerSequence);
            return slot;
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    Slot slot = consume();
                    if (slot == null) {
                        return;
                    }

                    Operation op = slot.op;
                    try {
                        op.setNodeEngine(nodeEngine);
                        op.setPartitionId(partitionId);
                        op.beforeRun();
                        op.run();

                        Object response = op.returnsResponse() ? op.getResponse() : null;
                        op.afterRun();
                        op.set(response, false);
                    } finally {
                        consumerSequenceRef.set(consumerSequenceRef.get() + 1);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Slot {
        //no need to have a atomiclong, could also be done with volatile field
        private final AtomicLong sequence = new AtomicLong(0);
        private Operation op;

        public void commit(long version) {
            sequence.set(version);
        }

        public void awaitCommitted(long consumerSequence) {
            for (; ; ) {
                if (sequence.get() >= consumerSequence) {
                    return;
                }
            }
        }
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        op.setServiceName(serviceName);
        op.setPartitionId(partitionId);
        PartitionOperationScheduler scheduler = schedulers[partitionId];
        scheduler.schedule(op);
        return op;
    }

    @Override
    public <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target) {
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
    public void runOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target) {
        throw new UnsupportedOperationException();
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
}
