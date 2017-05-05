/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Member;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PostJoinClientOperation extends AbstractClientOperation {

    private Map<String, String> mappings;

    public PostJoinClientOperation() {
    }

    public PostJoinClientOperation(Map<String, String> mappings) {
        this.mappings = mappings;
    }

    @Override
    public void run() throws Exception {
        if (mappings == null) {
            return;
        }

        ClientEngineImpl engine = getService();
        Set<Member> members = getNodeEngine().getClusterService().getMembers();
        HashSet<String> uuids = new HashSet<String>();
        for (Member member : members) {
            uuids.add(member.getUuid());
        }

        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            String ownerMemberUuid = entry.getValue();
            if (uuids.contains(ownerMemberUuid)) {
                engine.addOwnershipMapping(entry.getKey(), ownerMemberUuid);
            }
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
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        mappings = new HashMap<String, String>(len);
        for (int i = 0; i < len; i++) {
            String clientUuid = in.readUTF();
            String ownerUuid = in.readUTF();
            mappings.put(clientUuid, ownerUuid);
        }
    }

    @Override
    public int getId() {
        return ClientDataSerializerHook.POST_JOIN;
    }
}
