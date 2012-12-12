/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.MigrationServiceEvent;
import com.hazelcast.spi.PartitionLevelOperation;
import com.hazelcast.spi.impl.NodeServiceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Level;

public class FinalizeMigrationOperation extends AbstractOperation
        implements PartitionLevelOperation, MigrationCycleOperation {

    private MigrationEndpoint endpoint;     // source of destination
    private MigrationType type;       // move or copy
    private int copyBackReplicaIndex;
    private boolean success;

    public FinalizeMigrationOperation() {
    }

    public FinalizeMigrationOperation(final MigrationEndpoint endpoint, final MigrationType type,
                                      final int copyBackReplicaIndex, final boolean success) {
        this.endpoint = endpoint;
        this.success = success;
        this.type = type;
        this.copyBackReplicaIndex = copyBackReplicaIndex;
    }


    public void run() {
        final Collection<MigrationAwareService> services = getServices();
        final MigrationServiceEvent event = new MigrationServiceEvent(endpoint, getPartitionId(),
                getReplicaIndex(), type, copyBackReplicaIndex);
        for (MigrationAwareService service : services) {
            try {
                if (success) {
                    service.commitMigration(event);
                } else {
                    service.rollbackMigration(event);
                }
            } catch (Throwable e) {
                getNodeService().getLogger(FinalizeMigrationOperation.class.getName()).log(Level.WARNING,
                        "Error while finalizing migration -> " + event, e);
            }
        }
        PartitionService partitionService = getService();
        MigrationInfo migrationInfo = partitionService.removeActiveMigration(getPartitionId());

        if (success) {
            NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
            nodeService.onPartitionMigrate(migrationInfo);
        }
    }

    protected Collection<MigrationAwareService> getServices() {
        Collection<MigrationAwareService> services = new LinkedList<MigrationAwareService>();
        NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        for (Object serviceObject : nodeService.getServices(MigrationAwareService.class)) {
            if (serviceObject instanceof MigrationAwareService) {
                MigrationAwareService service = (MigrationAwareService) serviceObject;
                services.add(service);
            }
        }
        return services;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        success = in.readBoolean();
        copyBackReplicaIndex = in.readInt();
        endpoint = MigrationEndpoint.readFrom(in);
        type = MigrationType.readFrom(in);
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(success);
        out.writeInt(copyBackReplicaIndex);
        MigrationEndpoint.writeTo(endpoint, out);
        MigrationType.writeTo(type, out);
    }
}