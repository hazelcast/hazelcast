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

import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.security.Permission;

/**
 * Client request class for {@link java.util.Map#entrySet()} implementation
 */
public class ClientReplicatedMapEntrySetRequest
        extends AbstractReplicatedMapClientRequest {

    ClientReplicatedMapEntrySetRequest() {
        super(null);
    }

    public ClientReplicatedMapEntrySetRequest(String mapName) {
        super(mapName);
    }

    @Override
    public Object call()
            throws Exception {
        ReplicatedRecordStore recordStore = getReplicatedRecordStore();
        return new ReplicatedMapEntrySet(recordStore.entrySet());
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
