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
import java.util.ArrayList;
import java.util.Collection;

/**
 * Client request class for {@link java.util.Map#values()} implementation
 */
public class ClientReplicatedMapValuesRequest extends AbstractReplicatedMapClientRequest {

    ClientReplicatedMapValuesRequest() {
        super(null);
    }

    public ClientReplicatedMapValuesRequest(String mapName) {
        super(mapName);
    }

    @Override
    public Object call() throws Exception {
        ReplicatedMapService service = getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(getMapName());
        Collection<ReplicatedRecord> values = new ArrayList<ReplicatedRecord>();
        for (ReplicatedRecordStore store : stores) {
            values.addAll(store.values(false));
        }
        Collection<Data> dataValues = new ArrayList<Data>(values.size());
        for (ReplicatedRecord value : values) {
            dataValues.add(serializationService.toData(value.getValue()));
        }
        return new ReplicatedMapValueCollection(dataValues);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.VALUES;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getMapName(), ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        return "values";
    }
}
