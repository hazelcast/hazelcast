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

package com.hazelcast.partition.client;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientPortableHook;
import com.hazelcast.internal.partition.InternalPartitionService;

import java.security.Permission;

public class RemovePartitionLostListenerRequest
        extends BaseClientRemoveListenerRequest {

    public RemovePartitionLostListenerRequest() {
    }

    public RemovePartitionLostListenerRequest(String registrationId) {
        super(null, registrationId);
    }

    public Object call()
            throws Exception {
        final InternalPartitionService service = getService();
        return service.removePartitionLostListener(registrationId);
    }

    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.REMOVE_PARTITION_LOST_LISTENER;
    }
    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "removePartitionLostListener";
    }
}
