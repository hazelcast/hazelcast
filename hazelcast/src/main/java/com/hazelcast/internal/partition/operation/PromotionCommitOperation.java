/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.IPartitionService;
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
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.cluster.Versions.V6_0;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
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
 *     <li>In last stage, it fires legacy destination events when required and returns the response.</li>
 * </ul>
 *
 */
public class PromotionCommitOperation extends AbstractPartitionOperation implements MigrationCycleOperation {

    private PartitionRuntimeState partitionState;

    private Collection<MigrationInfo> promotions;

    private UUID expectedMemberUuid;

    private transient boolean success;
    private transient MigrationStateImpl migrationState;
    // Promotions whose BeforePromotionOperation failed. Set before the FINALIZE_PROMOTION stage.
    private transient Collection<MigrationInfo> failedBeforePromotions = Collections.emptyList();

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
                return finalizePromotion();
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
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        InternalPartitionServiceImpl partitionService = getService();

        if (!partitionService.getMigrationManager().acquirePromotionPermit()) {
            throw new RetryableHazelcastException("Another promotion is being run currently. "
                    + "This is only expected when promotion is retried to an unresponsive destination.");
        }

        long partitionStateStamp;
        try {
            partitionStateStamp = partitionService.getPartitionStateStamp();
            if (partitionState.getStamp() == partitionStateStamp) {
                return alreadyAppliedAllPromotions();
            }

            filterAlreadyAppliedPromotions();
            if (promotions.isEmpty()) {
                return alreadyAppliedAllPromotions();
            }

            partitionService.getMigrationInterceptor().onPromotionStart(MigrationParticipant.DESTINATION, promotions);
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(V6_0)) {
                migrationState = new MigrationStateImpl(Clock.currentTimeMillis(), promotions.size(), 0, 0L);
                partitionService.getPartitionEventManager().sendMigrationProcessStartedEvent(migrationState);
            }
        } catch (Throwable t) {
            // No BeforePromotionOperation was submitted yet, so there is nothing to roll back.
            releasePromotionPermit();
            throw rethrow(t);
        }

        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Submitting BeforePromotionOperations for " + promotions.size() + " promotions. "
                    + "Promotion partition state stamp: " + partitionState.getStamp()
                    + ", current partition state stamp: " + partitionStateStamp
            );
        }

        PromotionOperationCallback beforePromotionsCallback = new BeforePromotionOperationCallback(this, promotions.size());

        for (MigrationInfo promotion : promotions) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting BeforePromotionOperation for promotion: %s", promotion);
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
        releasePromotionPermit();
        success = true;
        return CallStatus.RESPONSE;
    }

    private void filterAlreadyAppliedPromotions() {
        ILogger logger = getLogger();
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager stateManager = partitionService.getPartitionStateManager();
        Iterator<MigrationInfo> iter = promotions.iterator();
        while (iter.hasNext()) {
            MigrationInfo promotion = iter.next();
            InternalPartitionImpl partition = stateManager.getPartitionImpl(promotion.getPartitionId());

            if (partition.version() >= promotion.getFinalPartitionVersion()) {
                logger.fine("Already applied promotion commit. -> %s", promotion);
                iter.remove();
            }
        }
    }

    /**
     * Processes the sent partition state and sends {@link FinalizePromotionOperation} for all promotions.
     * If a {@link BeforePromotionOperation} failed, rolls back the other promotions instead.
     */
    private CallStatus finalizePromotion() {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        OperationService operationService = nodeEngine.getOperationService();

        ILogger logger = getLogger();
        Collection<MigrationInfo> promotionsToFinalize = promotions;
        if (failedBeforePromotions.isEmpty()) {
            partitionState.setMaster(getCallerAddress());
            try {
                success = partitionService.processPartitionRuntimeState(partitionState);
            } catch (Throwable t) {
                // BeforePromotionOperations have set the migrating flags. FinalizePromotionOperations with a failed result
                // roll back the services and clear the flags, and the COMPLETE stage releases the promotion permit.
                logger.severe("Could not apply the partition state of the promotion, rolling back "
                        + promotions.size() + " promotions", t);
                success = false;
            }

            if (!success) {
                logger.severe("Promotion of " + promotions.size() + " partitions failed. "
                        + ". Promotion partition state stamp: " + partitionState.getStamp()
                        + ", current partition state stamp: " + partitionService.getPartitionStateStamp()
                );
            }
        } else {
            // Do not apply the partition state. Roll back only the promotions whose BeforePromotionOperation set
            // the migrating flag. The flag of a failed promotion belongs to another operation.
            logger.warning("Rolling back " + promotions.size() + " promotions, because " + failedBeforePromotions.size()
                    + " of them could not start: " + failedBeforePromotions);
            success = false;
            promotionsToFinalize = new ArrayList<>(promotions);
            promotionsToFinalize.removeAll(failedBeforePromotions);
            if (promotionsToFinalize.isEmpty()) {
                complete();
                return CallStatus.RESPONSE;
            }
        }

        if (logger.isFineEnabled()) {
            logger.fine("Submitting FinalizePromotionOperations for " + promotionsToFinalize.size() + " promotions. Result: "
                    + success + ". Promotion partition state stamp: " + partitionState.getStamp()
                    + ", current partition state stamp: " + partitionService.getPartitionStateStamp()
            );
        }

        PromotionOperationCallback finalizePromotionsCallback =
                new FinalizePromotionOperationCallback(this, promotionsToFinalize.size());

        for (MigrationInfo promotion : promotionsToFinalize) {
            if (logger.isFinestEnabled()) {
                logger.finest("Submitting FinalizePromotionOperation for promotion: %s. Result: %s", promotion, success);
            }
            Operation op = new FinalizePromotionOperation(promotion, success, finalizePromotionsCallback);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.execute(op);
        }
        return CallStatus.VOID;
    }

    private void complete() {
        InternalPartitionServiceImpl service = getService();
        try {
            service.getMigrationInterceptor().onPromotionComplete(MigrationParticipant.DESTINATION, promotions, success);
            if (migrationState != null) {
                PartitionEventManager eventManager = service.getPartitionEventManager();
                MigrationStateImpl ms = migrationState;
                for (MigrationInfo promotion : promotions) {
                    ms = ms.onComplete(1, 0L);
                    eventManager.sendMigrationEvent(ms, promotion, 0L);
                }
                eventManager.sendMigrationProcessCompletedEvent(ms);
            }
        } finally {
            releasePromotionPermit();
        }
    }

    private void releasePromotionPermit() {
        InternalPartitionServiceImpl service = getService();
        service.getMigrationManager().releasePromotionPermit();
    }

    /** Reruns this operation with next {@link #runStage}*/
    private void scheduleNextRun(RunStage nextState) {
        runStage = nextState;
        // The operation started execution in the BEFORE_PROMOTION stage, so the call timeout no longer applies
        // (see Operation#getCallTimeout). Otherwise the operation runner rejects a next stage that starts after
        // the call timeout, the promotion never completes and the promotion permit is never released.
        OperationAccessor.setCallTimeout(this, Long.MAX_VALUE);
        getNodeEngine().getOperationService().execute(this);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PROMOTION_COMMIT;
    }

    /**
     * Checks if all {@link BeforePromotionOperation}s have been executed, successfully or not.
     * On completion sets the {@link #runStage} to {@link RunStage#FINALIZE_PROMOTION}
     * and reschedules this {@link PromotionCommitOperation}.
     */
    private static class BeforePromotionOperationCallback implements PromotionOperationCallback {
        private final PromotionCommitOperation promotionCommitOperation;
        private final AtomicInteger tasks;
        private final Collection<MigrationInfo> failedPromotions = new ConcurrentLinkedQueue<>();

        BeforePromotionOperationCallback(PromotionCommitOperation promotionCommitOperation, int tasks) {
            this.promotionCommitOperation = promotionCommitOperation;
            this.tasks = new AtomicInteger(tasks);
        }

        @Override
        public void onComplete(MigrationInfo promotion) {
            onTaskDone(promotion);
        }

        @Override
        public void onFailure(MigrationInfo promotion) {
            failedPromotions.add(promotion);
            onTaskDone(promotion);
        }

        private void onTaskDone(MigrationInfo promotion) {
            int remainingTasks = tasks.decrementAndGet();

            ILogger logger = promotionCommitOperation.getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Completed before stage of %s. Remaining before promotion tasks: %s", promotion, remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All before promotion tasks are completed. Starting finalize promotion tasks...");
                if (!failedPromotions.isEmpty()) {
                    promotionCommitOperation.failedBeforePromotions = failedPromotions;
                }
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
                logger.finest("Completed finalize stage of %s. Remaining finalize promotion tasks: %s", promotion,
                        remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All finalize promotion tasks are completed.");
                promotionCommitOperation.scheduleNextRun(RunStage.COMPLETE);
            }
        }

        @Override
        public void onFailure(MigrationInfo promotion) {
            // FinalizePromotionOperation does not report failures, the finalize stage cannot be rolled back
            onComplete(promotion);
        }
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public String getServiceName() {
        return IPartitionService.SERVICE_NAME;
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

        /** Called when the operation of the promotion failed before it changed any state. */
        void onFailure(MigrationInfo promotion);
    }
}
