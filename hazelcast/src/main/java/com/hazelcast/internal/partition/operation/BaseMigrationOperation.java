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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.impl.InternalMigrationListener;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

public abstract class BaseMigrationOperation extends AbstractOperation
        implements MigrationCycleOperation, PartitionAwareOperation {

    protected MigrationInfo migrationInfo;
    protected boolean success;

    public BaseMigrationOperation() {
    }

    public BaseMigrationOperation(MigrationInfo migrationInfo) {
        this.migrationInfo = migrationInfo;
        setPartitionId(migrationInfo.getPartitionId());
    }

    @Override
    public final void beforeRun() throws Exception {
        try {
            onMigrationStart();
            verifyClusterState();
        } catch (Exception e) {
            onMigrationComplete(false);
            throw e;
        }
    }

    private void verifyClusterState() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ClusterState clusterState = nodeEngine.getClusterService().getClusterState();
        if (clusterState != ClusterState.ACTIVE) {
            throw new IllegalStateException("Cluster state is not active! " + clusterState);
        }
        final Node node = nodeEngine.getNode();
        if (!node.getNodeExtension().isStartCompleted()) {
            throw new IllegalStateException("Partition table received before startup is completed. "
                    + "Caller: " + getCallerAddress());
        }
    }

    protected void onMigrationStart() {
        InternalPartitionServiceImpl partitionService = getService();
        InternalMigrationListener migrationListener = partitionService.getInternalMigrationListener();
        migrationListener.onMigrationStart(getMigrationParticipantType(), migrationInfo);
    }

    protected void onMigrationComplete() {
        onMigrationComplete(success);
    }

    protected void onMigrationComplete(boolean result) {
        InternalPartitionServiceImpl partitionService = getService();
        InternalMigrationListener migrationListener = partitionService.getInternalMigrationListener();
        migrationListener.onMigrationComplete(getMigrationParticipantType(), migrationInfo, result);
    }

    protected abstract InternalMigrationListener.MigrationParticipant getMigrationParticipantType();

    public MigrationInfo getMigrationInfo() {
        return migrationInfo;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        if (!migrationInfo.isValid()) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        migrationInfo.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        migrationInfo = new MigrationInfo();
        migrationInfo.readData(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", migration=").append(migrationInfo);
    }
}
