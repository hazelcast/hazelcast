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

package com.hazelcast.impl.partition;

import com.hazelcast.impl.spi.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public class FinalizeMigrationOperation extends AbstractOperation implements PartitionLockFreeOperation {

    private boolean success;
    private MigrationEndpoint endpoint;

    public FinalizeMigrationOperation() {
    }

    public FinalizeMigrationOperation(final MigrationEndpoint endpoint, final boolean success) {
        this.endpoint = endpoint;
        this.success = success;
    }

    public void run() {
        Collection<MigrationAwareService> services = getServices();
        for (MigrationAwareService service : services) {
            if (success) {
                service.commitMigration(endpoint, getPartitionId(), getReplicaIndex());
            } else {
                service.rollbackMigration(endpoint, getPartitionId(), getReplicaIndex());
            }
//            System.err.println("Migration completed -> " + success + ". Endpoint -> " + endpoint
//                              + ". Service -> " + service
//                              + ". Partition -> " + getPartitionId() + ". Replica -> " + getReplicaIndex()
//                              + " T -> " + Thread.currentThread().getName());
        }
        getResponseHandler().sendResponse(null);
    }

    protected Collection<MigrationAwareService> getServices() {
        Collection<MigrationAwareService> services = new LinkedList<MigrationAwareService>();
        NodeServiceImpl nodeService = (NodeServiceImpl) getNodeService();
        for (Object serviceObject : nodeService.getServices().values()) {
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
        endpoint = MigrationEndpoint.get(in.readByte());
    }

    @Override
    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(success);
        out.writeByte(endpoint.getCode());
    }
}