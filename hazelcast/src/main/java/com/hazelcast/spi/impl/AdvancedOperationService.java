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

public class AdvancedOperationService implements OperationServiceImpl {

    private final NodeEngineImpl nodeEngine;
    private final Node node;
    private final PartitionOperationQueue[] schedulers;
    private final boolean localCallOptimizationEnabled;

    public AdvancedOperationService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.node = nodeEngine.getNode();
        int partitionCount = node.getGroupProperties().PARTITION_COUNT.getInteger();
        this.schedulers = new PartitionOperationQueue[partitionCount];
        int ringbufferSize = 1024;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            schedulers[partitionId] = new PartitionOperationQueue(partitionId, ringbufferSize);
        }
        this.localCallOptimizationEnabled = false;
    }

    public class PartitionThreadScheduler {

        private final PartitionThread[] threads;
        private final Slot[] ringbuffer;

        public PartitionThreadScheduler(int threadCount, int partitionCount) {
            this.threads = new PartitionThread[threadCount];
            for (int k = 0; k < threads.length; k++) {
                threads[k] = new PartitionThread();
            }

            this.ringbuffer = new Slot[partitionCount];
            for (int k = 0; k < ringbuffer.length; k++) {
                ringbuffer[k] = new Slot();
            }
        }

        public void schedule(PartitionOperationQueue scheduler) {

        }

        private class PartitionThread extends Thread {
            public void run() {

            }
        }

        private class Slot {
            private final AtomicLong sequence = new AtomicLong(0);
            private PartitionOperationQueue scheduler;
        }
    }

    /**
     * A Scheduler responsible for scheduling operations for a specific partitions.
     * The PartitionOperationQueue will guarantee that at any given moment, at
     * most 1 thread will be active in that partition.
     * <p/>
     * todo:
     * - deal with 'overflow' so overproducing..
     * the producer can run to where the consumer is. So all slots with have a value, lower than the current
     * consumersequence, can be used.
     * - improved thread assignment
     * - add batching for the consumer
     * - add system messages. System messages can be stored on a different regular queue (e.g. concurrentlinkedqueue)
     * <p/>
     * bad things:
     * - contention on the producersequenceref with concurrent producers
     * - when a consumer is finished, it needs to unset the scheduled bit on the producersequence,
     * this will cause contention of the producers with the consumers. This is actually also
     * the case with actors in akka. The nice thing however is that contention between producer
     * and condumer will not happen when there is a lot of work being processed since the scheduler
     * needs to remain 'scheduled'.
     * <p/>
     * workstealing: when a partitionthread is finished with running a partitionoperationscheduler,
     * instead of waiting for more work, it could try to 'steal' another partitionoperationscheduler
     * that has pending work.
     */
    public class PartitionOperationQueue implements Runnable {
        private final int partitionId;

        private final Slot[] ringbuffer;

        private final AtomicLong producerSeq = new AtomicLong(0);

        //we only have a single consumer
        private final AtomicLong consumerSeq = new AtomicLong(0);

        private final Executor executor = Executors.newFixedThreadPool(1);

        public PartitionOperationQueue(final int partitionId, int ringBufferSize) {
            this.partitionId = partitionId;
            this.ringbuffer = new Slot[ringBufferSize];

            for (int k = 0; k < ringbuffer.length; k++) {
                ringbuffer[k] = new Slot();
            }
        }

        public int toIndex(long sequence) {
            if (sequence % 2 == 1) {
                sequence--;
            }

            //todo: can be done more efficient by not using mod
            return (int) ((sequence / 2) % ringbuffer.length);
        }

        private int size(long producerSeq, long consumerSeq) {
            if (producerSeq % 2 == 1) {
                producerSeq--;
            }

            if (producerSeq == consumerSeq) {
                return 0;
            }

            return (int) ((producerSeq - consumerSeq) / 2);
        }

        public void schedule(Operation op) {
            try {
                long oldProducerSeq = producerSeq.get();
                long consumerSeq = this.consumerSeq.get();

                if (localCallOptimizationEnabled && oldProducerSeq == consumerSeq) {
                    //there currently is no pending work and the scheduler is not scheduled, so we can try to do a local runs optimization

                    long newProduceSequence = oldProducerSeq + 1;

                    //if we can set the 'uneven' flag, it means that scheduler is not yet running
                    if (producerSeq.compareAndSet(oldProducerSeq, newProduceSequence)) {
                        //we managed to signal other consumers that scheduling should not be done, because we do a local runs optimization

                        runOperation(op, true);

                        if (producerSeq.get() > newProduceSequence) {
                            //work has been produced by another producer, and since we still own the scheduled bit, we can safely
                            //schedule this
                            executor.execute(this);
                        } else {
                            //work has not yet been produced, so we are going to unset the scheduled bit.
                            if (producerSeq.compareAndSet(newProduceSequence, oldProducerSeq)) {
                                //we successfully managed to set the scheduled bit to false and no new work has been
                                //scheduled by other producers, so we are done.
                                return;
                            }

                            //new work has been scheduled by other producers, but since we still own the scheduled bit,
                            //we can schedule the work.
                            //work has been produced, so we need to offload it.
                            executor.execute(this);
                        }

                        return;
                    }

                    oldProducerSeq = producerSeq.get();
                } else if (size(oldProducerSeq, consumerSeq) == ringbuffer.length) {
                    //todo: overload
                    System.out.println("Overload");
                    throw new RuntimeException();
                }

                //this flag indicates if we need to schedule, or if scheduling already is taken care of.
                boolean schedule;
                long newProducerSeq;
                for (; ; ) {
                    newProducerSeq = oldProducerSeq + 2;
                    if (oldProducerSeq % 2 == 0) {
                        //if the scheduled flag is not set, we are going to be responsible for scheduling.
                        schedule = true;
                        newProducerSeq++;
                    } else {
                        //apparently this scheduler already is scheduled, so we don't need to schedule it.
                        schedule = false;
                    }

                    if (producerSeq.compareAndSet(oldProducerSeq, newProducerSeq)) {
                        break;
                    }

                    //we did not manage to claim the slot and potentially set the scheduled but, so we need to try again.
                    oldProducerSeq = producerSeq.get();
                }

                //we claimed a slot.
                int slotIndex = toIndex(newProducerSeq);
                Slot slot = ringbuffer[slotIndex];
                slot.op = op;
                slot.commit(newProducerSeq);

                if (schedule) {
                    executor.execute(this);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }
        }

        public Slot consume() {
            long consumerSeq = this.consumerSeq.get();
            long producerSeq = this.producerSeq.get();

            //todo: this can only run with the scheduled bit set..
            if ((consumerSeq == producerSeq) || (consumerSeq == producerSeq - 1)) {
                return null;
            }

            consumerSeq += 2;

            int slotIndex = toIndex(consumerSeq);
            Slot slot = ringbuffer[slotIndex];
            slot.awaitCommitted(consumerSeq);
            return slot;
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    Slot slot = consume();
                    if (slot == null) {
                        long oldProducerSeq = producerSeq.get();
                        if (oldProducerSeq % 2 == 0) {
                            throw new RuntimeException("scheduled bit expected");
                        }

                        //we unset the scheduled flag by subtracting one from the producerSeq
                        long newProducerSeq = consumerSeq.get();
                        if (producerSeq.compareAndSet(consumerSeq.get()+1, newProducerSeq)) {
                            //todo: it could have happened that work was produced after we last compared the consumerseq
                            //with the consumer sequence. So we need to check if the producersequence still is the same.
                            //also the oldProduce sequence can be determined on the consumesequence, prevening not noticing pending work
                            return;
                        }
                    } else {
                        Operation op = slot.op;
                        slot.op = null;
                        //todo: we don't need to update the slot-sequence?
                        try {
                            runOperation(op, false);
                        } finally {
                            consumerSeq.set(consumerSeq.get() + 2);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void runOperation(Operation op, boolean callerRuns) {
            try {
                op.setNodeEngine(nodeEngine);
                op.setPartitionId(partitionId);
                op.beforeRun();
                op.run();

                Object response = op.returnsResponse() ? op.getResponse() : null;
                op.afterRun();
                op.set(response, callerRuns);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private class Slot {
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
    }


    @Override
    public <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId) {
        op.setServiceName(serviceName);
        op.setPartitionId(partitionId);
        PartitionOperationQueue scheduler = schedulers[partitionId];
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
