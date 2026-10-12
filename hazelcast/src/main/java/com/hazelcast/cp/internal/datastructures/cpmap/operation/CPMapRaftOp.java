/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore.NO_TIMESTAMP;


public abstract class CPMapRaftOp extends RaftOp implements IdentifiedDataSerializable {
    protected String objectName;

    public CPMapRaftOp() {
    }

    public CPMapRaftOp(String objectName) {
        this.objectName = objectName;
    }

    @Override
    protected String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    /**
     * Returns leader's timestamp.
     * <p>
     * When this method is overridden by {@link
     * com.hazelcast.cp.internal.raft.impl.task.LeaderTimestampAware}
     * operations, the leader-assigned timestamp
     * is injected before execution/replication.
     * </p>
     *
     * @return leader's timestamp or {@code NO_TIMESTAMP}
     * if the operation is not timestamp-aware
     * @see com.hazelcast.cp.internal.raft.impl.task.LeaderTimestampAware
     * @see CPMapPurgeOp
     */
    protected long getLeaderTimestamp() {
        return NO_TIMESTAMP;
    }

    @Override
    public int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    protected CPMapStore getMap(CPGroupId groupId, String objectName) {
        CPMapService service = getService();
        return service.getOrInitMapStore(groupId, objectName);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(objectName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        objectName = in.readString();
    }
}
