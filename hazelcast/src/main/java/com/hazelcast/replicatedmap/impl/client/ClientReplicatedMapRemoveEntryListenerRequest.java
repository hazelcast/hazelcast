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

import com.hazelcast.client.BaseClientRemoveListenerRequest;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ReplicatedMapPermission;

import java.security.Permission;

/**
 * Client request class for {@link com.hazelcast.core.ReplicatedMap#removeEntryListener(String)} implementation
 */
public class ClientReplicatedMapRemoveEntryListenerRequest
        extends BaseClientRemoveListenerRequest {

    public ClientReplicatedMapRemoveEntryListenerRequest() {
    }

    public ClientReplicatedMapRemoveEntryListenerRequest(String mapName, String registrationId) {
        super(mapName, registrationId);
    }

    public Object call()
            throws Exception {
        final ReplicatedRecordStore replicatedRecordStore = getReplicatedRecordStore();
        return replicatedRecordStore.removeEntryListenerInternal(registrationId);
    }

    public int getClassId() {
        return ReplicatedMapPortableHook.REMOVE_LISTENER;
    }

    protected ReplicatedRecordStore getReplicatedRecordStore() {
        ReplicatedMapService replicatedMapService = getService();
        return replicatedMapService.getReplicatedRecordStore(name, true);
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "removeEntryListener";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{registrationId};
    }
}
