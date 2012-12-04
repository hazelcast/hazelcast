/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeServiceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class MigrationRequestOperation extends AbstractOperation implements PartitionLevelOperation {
    protected Address from;
    protected Address to;
    private boolean migration;  // migration or backup
    private boolean diffOnly;
    private int copyBackReplicaIndex = -1;

    private transient boolean success = false;

    public MigrationRequestOperation() {
    }

    public MigrationRequestOperation(int partitionId, Address from, Address to, int replicaIndex, boolean migration) {
        this(partitionId, from, to, replicaIndex, migration, false);
    }

    public MigrationRequestOperation(int partitionId, Address from, Address to, int replicaIndex,
                                     boolean migration, boolean diffOnly) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.from = from;
        this.to = to;
        this.migration = migration;
        this.diffOnly = diffOnly;
    }

    public MigrationInfo createMigrationInfo() {
        return new MigrationInfo(getPartitionId(), getReplicaIndex(), copyBackReplicaIndex,
                getMigrationType(), from, to);
    }

    public MigrationType getMigrationType() {
        return migration ? MigrationType.MOVE :
               (copyBackReplicaIndex < 0 ? MigrationType.COPY : MigrationType.MOVE_COPY_BACK);
    }

    public boolean isMigration() {
        return migration;
    }

    public int getCopyBackReplicaIndex() {
        return copyBackReplicaIndex;
    }

    public void setCopyBackReplicaIndex(final int copyBackReplicaIndex) {
        this.copyBackReplicaIndex = copyBackReplicaIndex;
    }

    public void setFromAddress(final Address from) {
        this.from = from;
    }

    public void run() {
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();
//        final ResponseHandler responseHandler = getResponseHandler();
        if (to.equals(from)) {
            getLogger().log(Level.FINEST, "To and from addresses are same! => " + toString());
//            responseHandler.sendResponse(Boolean.FALSE);
            success = false;
            return;
        }
        if (from == null) {
            getLogger().log(Level.FINEST, "From address is null => " + toString());
        }
        final PartitionService partitionService = getService();
        try {
            Member target = partitionService.getMember(to);
            if (target == null) {
                getLogger().log(Level.WARNING, "Target member of task could not be found! => " + toString());
//                responseHandler.sendResponse(Boolean.FALSE);
                success = false;
                return;
            }

            partitionService.addActiveMigration(createMigrationInfo());
            final NodeService nodeService = getNodeService();
            final long timeout = nodeService.getGroupProperties().PARTITION_MIGRATION_TIMEOUT.getLong();
            final Collection<Operation> tasks = prepareMigrationTasks(partitionId, replicaIndex);
//            nodeService.execute(new Runnable() {
//                public void run() {
//                    try {
//                        Invocation inv = nodeService.createInvocationBuilder(PartitionService.SERVICE_NAME,
//                                new MigrationOperation(partitionId, replicaIndex, copyBackReplicaIndex,
//                                        getMigrationType(), tasks, from), partitionId)
//                                .setTryCount(3).setTryPauseMillis(1000).setReplicaIndex(replicaIndex).setTarget(to)
//                                .build();
//                        Future future = inv.invoke();
//                        Boolean result = (Boolean) IOUtil.toObject(future.get(timeout, TimeUnit.SECONDS));
//                        responseHandler.sendResponse(result);
//                    } catch (Throwable e) {
//                        onError(responseHandler, e);
//                    }
//                }
//            });
            Invocation inv = nodeService.createInvocationBuilder(PartitionService.SERVICE_NAME,
                    new MigrationOperation(partitionId, replicaIndex, copyBackReplicaIndex,
                            getMigrationType(), tasks, from), partitionId)
                    .setTryCount(3).setTryPauseMillis(1000).setReplicaIndex(replicaIndex).setTarget(to)
                    .build();
            Future future = inv.invoke();
            success = (Boolean) IOUtil.toObject(future.get(timeout, TimeUnit.SECONDS));
//            responseHandler.sendResponse(result);
        } catch (Throwable e) {
            onError(e);
        }
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    private Collection<Operation> prepareMigrationTasks(final int partitionId, final int replicaIndex) {
        NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        final MigrationType migrationType = getMigrationType();
        final MigrationServiceEvent event = new MigrationServiceEvent(MigrationEndpoint.SOURCE,
                partitionId, replicaIndex, migrationType, copyBackReplicaIndex);
        final Collection<Operation> tasks = new LinkedList<Operation>();
        for (Object serviceObject : nodeService.getServices(MigrationAwareService.class)) {
            if (serviceObject instanceof MigrationAwareService) {
                MigrationAwareService service = (MigrationAwareService) serviceObject;
                final Operation op = service.prepareMigrationOperation(event);
                if (op != null) {
                    service.beforeMigration(event);
                    tasks.add(op);
                }
            }
        }
        return tasks;
    }

    private void onError(Throwable e) {
        Level level = Level.WARNING;
        if (e instanceof ExecutionException) {
            e = e.getCause();
        }
        if (e instanceof MemberLeftException || e instanceof IllegalStateException) {
            level = Level.FINEST;
        }
        getLogger().log(level, e.getMessage(), e);
//        responseHandler.sendResponse(Boolean.FALSE);
        success = false;
    }

    private ILogger getLogger() {
        return getNodeService().getLogger(MigrationRequestOperation.class.getName());
    }

    public void writeInternal(DataOutput out) throws IOException {
        from.writeData(out);
        to.writeData(out);
        out.writeBoolean(migration);
        out.writeBoolean(diffOnly);
        out.writeInt(copyBackReplicaIndex);
    }

    public void readInternal(DataInput in) throws IOException {
        from = new Address();
        from.readData(in);
        to = new Address();
        to.readData(in);
        migration = in.readBoolean();
        diffOnly = in.readBoolean();
        copyBackReplicaIndex = in.readInt();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigrationRequestOperation");
        sb.append("{partitionId=").append(getPartitionId());
        sb.append(", from=").append(from);
        sb.append(", to=").append(to);
        sb.append(", replicaIndex=").append(getReplicaIndex());
        sb.append(", migration=").append(migration);
        sb.append(", diffOnly=").append(diffOnly);
        sb.append(", copyBackReplicaIndex=").append(copyBackReplicaIndex);
        sb.append('}');
        return sb.toString();
    }

    public Address getFromAddress() {
        return from;
    }

    public Address getToAddress() {
        return to;
    }
}
