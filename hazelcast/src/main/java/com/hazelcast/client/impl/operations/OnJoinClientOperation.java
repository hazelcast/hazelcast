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

package com.hazelcast.client.impl.operations;

import com.hazelcast.client.impl.ClientDataSerializerHook;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.cp.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class OnJoinClientOperation extends AbstractClientOperation {

    private Map<UUID, UUID> mappings;

    public OnJoinClientOperation() {
    }

    public OnJoinClientOperation(Map<UUID, UUID> mappings) {
        this.mappings = mappings;
    }

    @Override
    public void run() throws Exception {
        if (mappings == null) {
            return;
        }
        ClientEngineImpl engine = getService();
        for (Map.Entry<UUID, UUID> entry : mappings.entrySet()) {
            engine.addOwnershipMapping(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String getServiceName() {
        return ClientEngineImpl.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (mappings == null) {
            out.writeInt(0);
            return;
        }
        int len = mappings.size();
        out.writeInt(len);
        for (Map.Entry<UUID, UUID> entry : mappings.entrySet()) {
            UUIDSerializationUtil.writeUUID(out, entry.getKey());
            UUIDSerializationUtil.writeUUID(out, entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        mappings = createHashMap(len);
        for (int i = 0; i < len; i++) {
            UUID clientUuid = UUIDSerializationUtil.readUUID(in);
            UUID ownerUuid = UUIDSerializationUtil.readUUID(in);
            mappings.put(clientUuid, ownerUuid);
        }
    }

    @Override
    public int getClassId() {
        return ClientDataSerializerHook.ON_JOIN;
    }
}
