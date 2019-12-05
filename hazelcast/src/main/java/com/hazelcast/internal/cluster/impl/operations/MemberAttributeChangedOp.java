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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;

public class MemberAttributeChangedOp extends AbstractClusterOperation {

    private MemberAttributeOperationType operationType;
    private String key;
    private String value;

    public MemberAttributeChangedOp() {
    }

    public MemberAttributeChangedOp(MemberAttributeOperationType operationType, String key, String value) {
        this.operationType = operationType;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        final ClusterServiceImpl cs = getService();
        cs.updateMemberAttribute(getCallerUuid(), operationType, key, value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeByte(operationType.getId());
        if (operationType == PUT) {
            out.writeUTF(value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        key = in.readUTF();
        operationType = MemberAttributeOperationType.getValue(in.readByte());
        if (operationType == PUT) {
            value = in.readUTF();
        }
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER_ATTR_CHANGED;
    }
}
