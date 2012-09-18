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

import com.hazelcast.spi.*;
import com.hazelcast.spi.MigrationServiceEvent.MigrationEndpoint;
import com.hazelcast.spi.MigrationServiceEvent.MigrationType;
import com.hazelcast.spi.impl.AbstractOperation;
import com.hazelcast.spi.impl.NodeServiceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Level;

import static com.hazelcast.spi.MigrationServiceEvent.MigrationEndpoint.*;
import static com.hazelcast.spi.MigrationServiceEvent.MigrationType.*;

public class FinalizeMigrationOperation extends AbstractOperation implements PartitionLockFreeOperation {

    private boolean source;     // source of destination
    private boolean move;       // move or copy
    private boolean success;

    public FinalizeMigrationOperation() {
    }

    public FinalizeMigrationOperation(final boolean source, final boolean move, final boolean success) {
        this.source = source;
        this.success = success;
        this.move = move;
    }


    public void run() {
        final Collection<MigrationAwareService> services = getServices();
        final MigrationEndpoint endpoint = source ? SOURCE : DESTINATION;
        final MigrationType type = move ? MOVE : COPY;
        final MigrationServiceEvent event = new MigrationServiceEvent(endpoint, getPartitionId(),
                getReplicaIndex(), type);
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
        partitionService.removeActiveMigration(getPartitionId());
    }

    protected Collection<MigrationAwareService> getServices() {
        Collection<MigrationAwareService> services = new LinkedList<MigrationAwareService>();
        NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        for (Object serviceObject : nodeService.getServices(MigrationAwareService.class, true)) {
            if (serviceObject instanceof MigrationAwareService) {
                MigrationAwareService service = (MigrationAwareService) serviceObject;
                services.add(service);
            }
        }
        return services;
    }

    @Override
    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        success = in.readBoolean();
        source = in.readBoolean();
        move = in.readBoolean();
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(success);
        out.writeBoolean(source);
        out.writeBoolean(move);
    }
}