/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;

public class AddIndexBackupOperation extends MapOperation implements BackupOperation, TargetAware {

    private static final MemberVersion V3_12_1 = MemberVersion.of(3, 12, 1);

    private String attributeName;
    private boolean ordered;

    private transient boolean targetSupported;

    public AddIndexBackupOperation() {
    }

    public AddIndexBackupOperation(String name, String attributeName, boolean ordered) {
        super(name);
        this.attributeName = attributeName;
        this.ordered = ordered;
    }

    @Override
    public void setTarget(Address address) {
        Member target = getNodeEngine().getClusterService().getMember(address);
        if (target == null) {
            this.targetSupported = false;
        } else {
            MemberVersion memberVersion = target.getVersion();
            this.targetSupported = memberVersion.compareTo(V3_12_1) >= 0;
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
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
        if (targetSupported) {
            super.writeInternal(out);
            out.writeUTF(attributeName);
            out.writeBoolean(ordered);
        } else {
            // RU_COMPAT_3_12_0: here we are serializing the operation as a
            // no-op EvictBatchBackupOperation, so old pre 3.12.1 members are
            // not failing with exceptions.

            super.writeInternal(out);
            out.writeUTF(name);
            out.writeInt(0);
            out.writeInt(Integer.MAX_VALUE);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        attributeName = in.readUTF();
        ordered = in.readBoolean();
    }

    @Override
    public int getId() {
        // RU_COMPAT_3_12_0
        return targetSupported ? MapDataSerializerHook.ADD_INDEX_BACKUP : MapDataSerializerHook.EVICT_BATCH_BACKUP;
    }

}
