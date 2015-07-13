/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import java.security.Permission;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Client request class for {@link java.util.Map#entrySet()} implementation
 */
public class ClientReplicatedMapEntrySetRequest extends AbstractReplicatedMapClientRequest {

    ClientReplicatedMapEntrySetRequest() {
        super(null);
    }

    public ClientReplicatedMapEntrySetRequest(String mapName) {
        super(mapName);
    }

    @Override
    public Object call() throws Exception {
        ReplicatedMapService service = getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getMapName());
        Set<Map.Entry<Object, ReplicatedRecord>> entrySet = new HashSet<Map.Entry<Object, ReplicatedRecord>>();
        for (ReplicatedRecordStore store : stores) {
            entrySet.addAll(store.entrySet(false));
        }
        Set<Map.Entry<Data, Data>> entries = new HashSet<Map.Entry<Data, Data>>(entrySet.size());
        for (Map.Entry<Object, ReplicatedRecord> entry : entrySet) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue().getValue());
            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
        }
        return new ReplicatedMapEntrySet(entries);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.ENTRY_SET;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "entrySet";
    }
}
