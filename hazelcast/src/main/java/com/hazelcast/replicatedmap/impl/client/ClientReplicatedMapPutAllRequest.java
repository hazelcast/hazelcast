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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.io.IOException;
import java.security.Permission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Client request class for {@link Map#putAll(java.util.Map)} implementation
 */
public class ClientReplicatedMapPutAllRequest
        extends AbstractReplicatedMapClientRequest {

    private ReplicatedMapEntrySet entrySet;

    ClientReplicatedMapPutAllRequest() {
        super(null);
    }

    public ClientReplicatedMapPutAllRequest(String mapName, ReplicatedMapEntrySet entrySet) {
        super(mapName);
        this.entrySet = entrySet;
    }

    @Override
    public Object call()
            throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        Set<Map.Entry> entries = entrySet.getEntrySet();
        for (Map.Entry entry : entries) {
            recordStore.put(entry.getKey(), entry.getValue());
        }
        return null;
    }

    @Override
    public void write(PortableWriter writer)
            throws IOException {
        super.write(writer);
        entrySet.writePortable(writer);
    }

    @Override
    public void read(PortableReader reader)
            throws IOException {
        super.read(reader);
        entrySet = new ReplicatedMapEntrySet();
        entrySet.readPortable(reader);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.PUT_ALL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return "putAll";
    }

    @Override
    public Object[] getParameters() {
        final Set<Map.Entry> set = entrySet.getEntrySet();
        final HashMap map = new HashMap();
        for (Map.Entry entry : set) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new Object[]{map};
    }
}
