/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Level;

// runs locally...
// can be PartitionLevelOperation if partition lock is re-entrant!
final class FinalizeMigrationOperation extends AbstractOperation
        implements /*PartitionLevelOperation, */ MigrationCycleOperation {

    private MigrationEndpoint endpoint;     // source of destination
    private boolean success;

    public FinalizeMigrationOperation() {
    }

    public FinalizeMigrationOperation(final MigrationEndpoint endpoint, final boolean success) {
        this.endpoint = endpoint;
        this.success = success;
    }

    public void run() {
        final Collection<MigrationAwareService> services = getServices();
        final PartitionMigrationEvent event = new PartitionMigrationEvent(endpoint, getPartitionId());
        for (MigrationAwareService service : services) {
            try {
                if (success) {
                    service.commitMigration(event);
                } else {
                    service.rollbackMigration(event);
                }
            } catch (Throwable e) {
                getNodeEngine().getLogger(FinalizeMigrationOperation.class.getName()).log(Level.WARNING,
                        "Error while finalizing migration -> " + event, e);
            }
        }
        PartitionServiceImpl partitionService = getService();
        MigrationInfo migrationInfo = partitionService.removeActiveMigration(getPartitionId());

        if (success) {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            nodeEngine.onPartitionMigrate(migrationInfo);
        }
    }

    protected Collection<MigrationAwareService> getServices() {
        Collection<MigrationAwareService> services = new LinkedList<MigrationAwareService>();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        for (Object serviceObject : nodeEngine.getServices(MigrationAwareService.class)) {
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
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        success = in.readBoolean();
        endpoint = MigrationEndpoint.readFrom(in);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(success);
        MigrationEndpoint.writeTo(endpoint, out);
    }
}