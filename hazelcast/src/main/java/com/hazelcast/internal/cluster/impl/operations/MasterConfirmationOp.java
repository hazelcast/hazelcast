/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.MembersViewMetadata;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

/**
 * @deprecated in 3.10
 */
@Deprecated
// Master confirmation logic is not needed in 3.10. This operation is still here to remain compatible with 3.9.
// RU_COMPAT_39: Do not remove Versioned interface!
// Version info is needed on 3.9 members while deserializing the operation.
public class MasterConfirmationOp extends AbstractClusterOperation implements Versioned {

    private MembersViewMetadata membersViewMetadata;

    private long timestamp;

    public MasterConfirmationOp() {
    }

    public MasterConfirmationOp(MembersViewMetadata membersViewMetadata, long timestamp) {
        this.membersViewMetadata = membersViewMetadata;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(membersViewMetadata);
        out.writeLong(timestamp);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        membersViewMetadata = in.readObject();
        timestamp = in.readLong();
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MASTER_CONFIRM;
    }
}
