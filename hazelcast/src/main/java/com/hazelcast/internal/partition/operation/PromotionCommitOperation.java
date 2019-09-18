/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Used for committing a promotion on destination. Sent by the master to update the partition table on destination and
 * commit the promotion.
 * The promotion is executed in two stages which are denoted by the {@link #beforeStateCompleted} property. First it invokes
 * {@link BeforePromotionOperation}s for every promoted partition. After all operations return it will reschedule itself
 * and finalize the promotions by sending {@link FinalizePromotionOperation} for every promotion.
 */
public class PromotionCommitOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    private Collection<MigrationInfo> promotions;

    private String expectedMemberUuid;

    private transient boolean success;

    // Used while PromotionCommitOperation is running to separate before and after phases
    private transient boolean beforeStateCompleted;

    public PromotionCommitOperation() {
    }

    public PromotionCommitOperation(PartitionRuntimeState partitionState, Collection<MigrationInfo> promotions,
            String expectedMemberUuid) {
        Preconditions.checkNotNull(promotions);
        this.partitionState = partitionState;
        this.promotions = promotions;
        this.expectedMemberUuid = expectedMemberUuid;
    }

    @Override
    public void beforeRun() throws Exception {
        if (beforeStateCompleted) {
            return;
        }

        NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        if (!localMember.getUuid().equals(expectedMemberUuid)) {
            throw new IllegalStateException("This " + localMember
                    + " is promotion commit destination but most probably it's restarted "
                    + "and not the expected target.");
        }

        Address masterAddress = nodeEngine.getMasterAddress();
        Address callerAddress = getCallerAddress();
        if (!callerAddress.equals(masterAddress)) {
            throw new IllegalStateException("Caller is not master node! Caller: " + callerAddress
                + ", Master: " + masterAddress);
        }
    }

    @Override
    public CallStatus call() throws Exception {
        if (!beforeStateCompleted) {
            return beforePromotion();
        } else {
            finalizePromotion();
            return CallStatus.DONE_RESPONSE;
        }
    }

    /**
     * Sends {@link BeforePromotionOperation}s for all promotions and register a callback on each operation to track when
     * operations are finished.
     */
    private CallStatus beforePromotion() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        InternalPartitionServiceImpl partitionService = getService();

        if (!partitionService.getMigrationManager().acquirePromotionPermit()) {
            throw new RetryableHazelcastException("Another promotion is being run currently. "
                    + "This is only expected when promotion is retried to an unresponsive destination.");
        }

        ILogger logger = getLogger();
        int partitionStateVersion = partitionService.getPartitionStateVersion();
        if (partitionState.getVersion() <= partitionStateVersion) {
            logger.warning("Already applied promotions to the partition state. Promotion state version: "
                    + partitionState.getVersion() + ", current version: " + partitionStateVersion);
            partitionService.getMigrationManager().releasePromotionPermit();
            success = true;
            return CallStatus.DONE_RESPONSE;
        }

        partitionService.getMigrationInterceptor().onPromotionStart(MigrationParticipant.DESTINATION, promotions);

        if (logger.isFineEnabled()) {
            logger.fine("Submitting BeforePromotionOperations for " + promotions.size() + " promotions. "
                    + "Promotion partition state version: " + partitionState.getVersion()
                    + ", current partition state version: " + partitionStateVersion);
        }

        Runnable beforePromotionsCallback = new BeforePromotionOperationCallback(this, new AtomicInteger(promotions.size()));

        for (MigrationInfo promotion : promotions) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting BeforePromotionOperation for promotion: " + promotion);
            }
            BeforePromotionOperation op = new BeforePromotionOperation(promotion, beforePromotionsCallback);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.execute(op);
        }
        return CallStatus.DONE_VOID;
    }

    /** Processes the sent partition state and sends {@link FinalizePromotionOperation} for all promotions. */
    private void finalizePromotion() {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        OperationService operationService = nodeEngine.getOperationService();

        partitionState.setMaster(getCallerAddress());
        success = partitionService.processPartitionRuntimeState(partitionState);

        ILogger logger = getLogger();
        if (!success) {
            logger.severe("Promotion of " + promotions.size() + " partitions failed. "
                    + ". Promotion partition state version: " + partitionState.getVersion()
                    + ", current partition state version: " + partitionService.getPartitionStateVersion());
        }
        if (logger.isFineEnabled()) {
            logger.fine("Submitting FinalizePromotionOperations for " + promotions.size() + " promotions. Result: " + success
                    + ". Promotion partition state version: " + partitionState.getVersion()
                    + ", current partition state version: " + partitionService.getPartitionStateVersion());
        }
        for (MigrationInfo promotion : promotions) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting FinalizePromotionOperation for promotion: " + promotion + ". Result: " + success);
            }
            FinalizePromotionOperation op = new FinalizePromotionOperation(promotion, success);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.execute(op);
        }
        partitionService.getMigrationInterceptor()
                .onPromotionComplete(MigrationParticipant.DESTINATION, promotions, success);
        partitionService.getMigrationManager().releasePromotionPermit();
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PROMOTION_COMMIT;
    }

    /**
     * Checks if all {@link BeforePromotionOperation}s have been executed.
     * On completion sets the {@link #beforeStateCompleted} to {@code true} and reschedules this {@link PromotionCommitOperation}.
     */
    private static class BeforePromotionOperationCallback implements Runnable {
        private final PromotionCommitOperation promotionCommitOperation;
        private final AtomicInteger tasks;

        BeforePromotionOperationCallback(PromotionCommitOperation promotionCommitOperation, AtomicInteger tasks) {
            this.promotionCommitOperation = promotionCommitOperation;
            this.tasks = tasks;
        }

        @Override
        public void run() {
            final int remainingTasks = tasks.decrementAndGet();

            ILogger logger = promotionCommitOperation.getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Remaining before promotion tasks: " + remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All before promotion tasks are completed.");
                promotionCommitOperation.onBeforePromotionsComplete();
            }
        }
    }

    /** Reruns this operation with {@link #beforeStateCompleted} set to {@code true}. */
    private void onBeforePromotionsComplete() {
        beforeStateCompleted = true;
        getNodeEngine().getOperationService().execute(this);
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        expectedMemberUuid = in.readUTF();
        partitionState = in.readObject();
        promotions = readCollection(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(expectedMemberUuid);
        out.writeObject(partitionState);
        writeCollection(promotions, out);
    }
}
