/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

public class AddIndexBackupOperation extends MapOperation implements BackupOperation {

    private String attributeName;
    private boolean ordered;

    public AddIndexBackupOperation() {
    }

    public AddIndexBackupOperation(String name, String attributeName, boolean ordered) {
        super(name);
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void runInternal() {
        int partitionId = getPartitionId();

        Indexes indexes = mapContainer.getIndexes(partitionId);
        indexes.recordIndexDefinition(attributeName, ordered);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(attributeName);
        out.writeBoolean(ordered);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        attributeName = in.readUTF();
        ordered = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ADD_INDEX_BACKUP;
    }

}
