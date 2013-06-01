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

package com.hazelcast.partition.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionDataSerializerHook;
import com.hazelcast.partition.PartitionServiceImpl;

import java.io.IOException;

/**
 * @mdogan 5/13/13
 */
public final class AddMigrationListenerRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        PartitionServiceImpl service = getService();
        service.addMigrationListener(new MigrationListener() {
            public void migrationStarted(MigrationEvent migrationEvent) {

            }

            public void migrationCompleted(MigrationEvent migrationEvent) {

            }

            public void migrationFailed(MigrationEvent migrationEvent) {

            }
        });
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.ADD_LISTENER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
