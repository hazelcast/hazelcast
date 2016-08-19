/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used for committing a promotion on destination.
 * Updates the partition table on destination and commits the promotion.
 */
public class PromotionCommitOperation extends Operation implements MigrationCycleOperation {

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
        super.beforeRun();

        NodeEngine nodeEngine = getNodeEngine();
        final Member localMember = nodeEngine.getLocalMember();
        if (!localMember.getUuid().equals(expectedMemberUuid)) {
            throw new IllegalStateException("This " + localMember
                    + " is promotion commit destination but most probably it's restarted "
                    + "and not the expected target.");
        }
    }

    @Override
    public void run() {
        if (beforeStateCompleted) {
            finalizePromotion();
        }
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        if (!beforeStateCompleted) {
            // Triggering before-promotion tasks in afterRun() after response phase is done,
            // to avoid inadvertently reading `beforeStateCompleted` as true when asked to send a response.
            // `beforeStateCompleted` will be set when all before-promotion tasks are completed.
            beforePromotion();
        }
    }

    private void beforePromotion() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalOperationService operationService = nodeEngine.getOperationService();
        AtomicInteger tasks = new AtomicInteger(promotions.size());

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Submitting before promotion tasks for " + promotions);
        } else if (logger.isFineEnabled()) {
            logger.fine("Submitting before promotion tasks for " + promotions.size() + " promotions.");
        }
        for (MigrationInfo promotion : promotions) {
            operationService.execute(new BeforePromotionTask(this, promotion, nodeEngine, tasks));
        }
    }

    private void finalizePromotion() {
        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Running promotion finalization for " + promotions.size() + " promotions.");
        }
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        OperationService operationService = nodeEngine.getOperationService();

        partitionState.setEndpoint(getCallerAddress());
        success = partitionService.processPartitionRuntimeState(partitionState);

        if (logger.isFinestEnabled()) {
            logger.finest("Submitting finalize promotion operations for " + promotions);
        } else if (logger.isFineEnabled()) {
            logger.fine("Submitting finalize promotion operations for " + promotions.size() + " promotions.");
        }
        for (MigrationInfo promotion : promotions) {
            int currentReplicaIndex = promotion.getDestinationCurrentReplicaIndex();
            FinalizePromotionOperation op = new FinalizePromotionOperation(currentReplicaIndex, success);
            op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine).setService(partitionService);
            operationService.executeOperation(op);
        }
    }

    private static class BeforePromotionTask implements PartitionSpecificRunnable {
        private final PromotionCommitOperation promotionCommitOperation;
        private final MigrationInfo promotion;
        private final NodeEngineImpl nodeEngine;
        private final AtomicInteger tasks;

        BeforePromotionTask(PromotionCommitOperation promotionCommitOperation, MigrationInfo promotion,
                NodeEngineImpl nodeEngine, AtomicInteger tasks) {
            this.promotionCommitOperation = promotionCommitOperation;
            this.promotion = promotion;
            this.nodeEngine = nodeEngine;
            this.tasks = tasks;
        }

        @Override
        public void run() {
            try {
                int currentReplicaIndex = promotion.getDestinationCurrentReplicaIndex();
                BeforePromotionOperation op = new BeforePromotionOperation(currentReplicaIndex);
                op.setPartitionId(promotion.getPartitionId()).setNodeEngine(nodeEngine)
                        .setService(nodeEngine.getPartitionService());

                InternalOperationService operationService = nodeEngine.getOperationService();
                operationService.runOperationOnCallingThread(op);
            } finally {
                completeTask();
            }
        }

        private void completeTask() {
            final int remainingTasks = tasks.decrementAndGet();

            ILogger logger = nodeEngine.getLogger(getClass());
            if (logger.isFinestEnabled()) {
                logger.finest("Remaining before promotion tasks: " + remainingTasks);
            }

            if (remainingTasks == 0) {
                logger.fine("All before promotion tasks are completed, re-submitting PromotionCommitOperation.");
                promotionCommitOperation.beforeStateCompleted = true;
                nodeEngine.getOperationService().executeOperation(promotionCommitOperation);
            }
        }

        @Override
        public int getPartitionId() {
            return promotion.getPartitionId();
        }
    }

    @Override
    public boolean returnsResponse() {
        return beforeStateCompleted;
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
        partitionState = new PartitionRuntimeState();
        partitionState.readData(in);

        int len = in.readInt();
        if (len > 0) {
            promotions = new ArrayList<MigrationInfo>(len);
            for (int i = 0; i < len; i++) {
                MigrationInfo migrationInfo = new MigrationInfo();
                migrationInfo.readData(in);
                promotions.add(migrationInfo);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(expectedMemberUuid);
        partitionState.writeData(out);

        int len = promotions.size();
        out.writeInt(len);
        for (MigrationInfo migrationInfo : promotions) {
            migrationInfo.writeData(out);
        }
    }
}
