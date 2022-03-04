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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor.MigrationParticipant;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionEventManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;

/**
 * Used for committing a promotion on destination. Sent by the master to update the partition table on destination and
 * commit the promotion.
 * The promotion is executed in three stages which are denoted by the {@link #runStage} property.
 * <ul>
 *     <li>First it invokes {@link BeforePromotionOperation}s for every promoted partition.
 *     After all operations return it will reschedule itself with {@link RunStage#FINALIZE_PROMOTION}.</li>
 *     <li>In {@link RunStage#FINALIZE_PROMOTION} stage, finalize the promotions by sending
 *     {@link FinalizePromotionOperation} for every promotion. After all complete, it will reschedule
 *     itself with {@link RunStage#COMPLETE}.</li>
 *     <li>In last stage, it will fire completion events and return the response.</li>
 * </ul>
 *
 */
public class PromotionCommitOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    private Collection<MigrationInfo> promotions;

    private UUID expectedMemberUuid;

    private transient boolean success;
    private transient MigrationStateImpl migrationState;

    // Used while PromotionCommitOperation is running to separate before and after phases
    private transient RunStage runStage = RunStage.BEFORE_PROMOTION;

    public PromotionCommitOperation() {
    }

    public PromotionCommitOperation(PartitionRuntimeState partitionState, Collection<MigrationInfo> promotions,
            UUID expectedMemberUuid) {
        Preconditions.checkNotNull(promotions);
        this.partitionState = partitionState;
        this.promotions = promotions;
        this.expectedMemberUuid = expectedMemberUuid;
    }

    @Override
    public void beforeRun() throws Exception {
        if (runStage != RunStage.BEFORE_PROMOTION) {
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
        Address caller = getCallerAddress();
        if (!caller.equals(masterAddress)) {
            throw new IllegalStateException("Caller is not master node! Caller: " + caller + ", Master: " + masterAddress);
        }

        InternalPartitionServiceImpl partitionService = getService();
        if (!partitionService.isMemberMaster(caller)) {
            throw new RetryableHazelcastException("Caller is not master node known by migration system! Caller: " + caller);
        }
    }

    @Override
    public CallStatus call() throws Exception {
        switch (runStage) {
            case BEFORE_PROMOTION:
                return beforePromotion();
            case FINALIZE_PROMOTION:
                finalizePromotion();
                return CallStatus.VOID;
            case COMPLETE:
                complete();
                return CallStatus.RESPONSE;
            default:
                throw new IllegalStateException("Unknown state: " + runStage);
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

        long partitionStateStamp;
        partitionStateStamp = partitionService.getPartitionStateStamp();
        if (partitionState.getStamp() == partitionStateStamp) {
            return alreadyAppliedAllPromotions();
        }

        filterAlreadyAppliedPromotions();
        if (promotions.isEmpty()) {
            return alreadyAppliedAllPromotions();
        }

        ILogger logger = getLogger();
        migrationState = new MigrationStateImpl(Clock.currentTimeMillis(), promotions.size(), 0, 0L);
        partitionService.getMigrationInterceptor().onPromotionStart(MigrationParticipant.DESTINATION, promotions);
        partitionService.getPartitionEventManager().sendMigrationProcessStartedEvent(migrationState);

        if (logger.isFineEnabled()) {
            logger.fine("Submitting BeforePromotionOperations for " + promotions.size() + " promotions. "
                    + "Promotion partition state stamp: " + partitionState.getStamp()
                    + ", current partition state stamp: " + partitionStateStamp
            );
        }

        PromotionOperationCallback beforePromotionsCallback = new BeforePromotionOperationCallback(this, promotions.size());

        for (MigrationInfo promotion : promotions) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting BeforePromotionOperation for promotion: " + promotion);
            }
            Operation op = new BeforePromotionOperation(promotion, beforePromotionsCallback);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.execute(op);
        }
        return CallStatus.VOID;
    }

    private CallStatus alreadyAppliedAllPromotions() {
        getLogger().warning("Already applied all promotions to the partition state. Promotion state stamp: "
                + partitionState.getStamp());
        InternalPartitionServiceImpl partitionService = getService();
        partitionService.getMigrationManager().releasePromotionPermit();
        success = true;
        return CallStatus.RESPONSE;
    }

    private void filterAlreadyAppliedPromotions() {
        if (getNodeEngine().getClusterService().getClusterVersion().isUnknownOrLessOrEqual(Versions.V4_0)) {
            return;
        }

        ILogger logger = getLogger();
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager stateManager = partitionService.getPartitionStateManager();
        Iterator<MigrationInfo> iter = promotions.iterator();
        while (iter.hasNext()) {
            MigrationInfo promotion = iter.next();
            InternalPartitionImpl partition = stateManager.getPartitionImpl(promotion.getPartitionId());

            if (partition.version() >= promotion.getFinalPartitionVersion()) {
                logger.fine("Already applied promotion commit. -> " + promotion);
                iter.remove();
            }
        }
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
                    + ". Promotion partition state stamp: " + partitionState.getStamp()
                    + ", current partition state stamp: " + partitionService.getPartitionStateStamp()
            );
        }
        if (logger.isFineEnabled()) {
            logger.fine("Submitting FinalizePromotionOperations for " + promotions.size() + " promotions. Result: " + success
                    + ". Promotion partition state stamp: " + partitionState.getStamp()
                    + ", current partition state stamp: " + partitionService.getPartitionStateStamp()
            );
        }

        PromotionOperationCallback finalizePromotionsCallback = new FinalizePromotionOperationCallback(this, promotions.size());

        for (MigrationInfo promotion : promotions) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting FinalizePromotionOperation for promotion: " + promotion + ". Result: " + success);
            }
            Operation op = new FinalizePromotionOperation(promotion, success, finalizePromotionsCallback);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.execute(op);
        }
    }

    private void complete() {
        InternalPartitionServiceImpl service = getService();
        service.getMigrationInterceptor().onPromotionComplete(MigrationParticipant.DESTINATION, promotions, success);
        PartitionEventManager eventManager = service.getPartitionEventManager();
        MigrationStateImpl ms = migrationState;
        for (MigrationInfo promotion : promotions) {
            ms = ms.onComplete(1, 0L);
            eventManager.sendMigrationEvent(ms, promotion, 0L);
        }
        eventManager.sendMigrationProcessCompletedEvent(ms);
        service.getMigrationManager().releasePromotionPermit();
    }

    /** Reruns this operation with next {@link #runStage}*/
    private void scheduleNextRun(RunStage nextState) {
        runStage = nextState;
        getNodeEngine().getOperationService().execute(this);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PROMOTION_COMMIT;
    }

    /**
     * Checks if all {@link BeforePromotionOperation}s have been executed.
     * On completion sets the {@link #runStage} to {@link RunStage#FINALIZE_PROMOTION}
     * and reschedules this {@link PromotionCommitOperation}.
     */
    private static class BeforePromotionOperationCallback implements PromotionOperationCallback {
        private final PromotionCommitOperation promotionCommitOperation;
        private final AtomicInteger tasks;

        BeforePromotionOperationCallback(PromotionCommitOperation promotionCommitOperation, int tasks) {
            this.promotionCommitOperation = promotionCommitOperation;
            this.tasks = new AtomicInteger(tasks);
        }

        @Override
        public void onComplete(MigrationInfo promotion) {
            int remainingTasks = tasks.decrementAndGet();

            ILogger logger = promotionCommitOperation.getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Completed before stage of " + promotion + ". Remaining before promotion tasks: " + remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All before promotion tasks are completed. Starting finalize promotion tasks...");
                promotionCommitOperation.scheduleNextRun(RunStage.FINALIZE_PROMOTION);
            }
        }
    }

    /**
     * Checks if all {@link FinalizePromotionOperation}s have been executed.
     * On completion sets the {@link #runStage} to {@link RunStage#COMPLETE}
     * and reschedules this {@link PromotionCommitOperation}.
     */
    private static class FinalizePromotionOperationCallback implements PromotionOperationCallback {
        private final PromotionCommitOperation promotionCommitOperation;
        private final AtomicInteger tasks;

        FinalizePromotionOperationCallback(PromotionCommitOperation promotionCommitOperation, int tasks) {
            this.promotionCommitOperation = promotionCommitOperation;
            this.tasks = new AtomicInteger(tasks);
        }

        @Override
        public void onComplete(MigrationInfo promotion) {
            int remainingTasks = tasks.decrementAndGet();

            ILogger logger = promotionCommitOperation.getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Completed finalize stage of " + promotion
                        + ". Remaining finalize promotion tasks: " + remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All finalize promotion tasks are completed.");
                promotionCommitOperation.scheduleNextRun(RunStage.COMPLETE);
            }
        }
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
        expectedMemberUuid = UUIDSerializationUtil.readUUID(in);
        partitionState = in.readObject();
        promotions = readCollection(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, expectedMemberUuid);
        out.writeObject(partitionState);
        writeCollection(promotions, out);
    }

    private enum RunStage {
        BEFORE_PROMOTION, FINALIZE_PROMOTION, COMPLETE
    }

    interface PromotionOperationCallback {
        void onComplete(MigrationInfo promotion);
    }
}
