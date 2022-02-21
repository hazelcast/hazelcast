/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.spi.blocking.operation;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.datastructures.RaftDataServiceDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.spi.blocking.AbstractBlockingService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Expires the given wait keys on the given blocking service. Invocations that
 * are waiting responses of these wait keys will be notified with
 * the return value provided by
 * {@link AbstractBlockingService#expiredWaitKeyResponse()}
 */
public class ExpireWaitKeysOp extends RaftOp implements IdentifiedDataSerializable {

    private String serviceName;
    private Collection<BiTuple<String, UUID>> keys;

    public ExpireWaitKeysOp() {
    }

    public ExpireWaitKeysOp(String serviceName, Collection<BiTuple<String, UUID>> keys) {
        this.serviceName = serviceName;
        this.keys = keys;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        AbstractBlockingService service = getService();
        service.expireWaitKeys(groupId, keys);
        return null;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public int getFactoryId() {
        return RaftDataServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataServiceDataSerializerHook.EXPIRE_WAIT_KEYS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(serviceName);
        out.writeInt(keys.size());
        for (BiTuple<String, UUID> key : keys) {
            out.writeString(key.element1);
            writeUUID(out, key.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readString();
        int size = in.readInt();
        keys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            UUID invocationUid = readUUID(in);
            keys.add(BiTuple.of(name, invocationUid));
        }
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", keys=").append(keys);
    }
}
