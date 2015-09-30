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

import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import java.security.Permission;

/**
 * Client request class for {@link java.util.Map#clear()} implementation
 */
public class ClientReplicatedMapClearRequest extends AbstractReplicatedMapClientRequest {

    ClientReplicatedMapClearRequest() {
        super(null);
    }

    public ClientReplicatedMapClearRequest(String mapName) {
        super(mapName);
    }

    @Override
    public Object call() throws Exception {
        ReplicatedMapService service = getService();
        service.clearLocalAndRemoteRecordStores(getMapName());
        return Boolean.TRUE;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.CLEAR;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "clear";
    }
}
