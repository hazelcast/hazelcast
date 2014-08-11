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

package com.hazelcast.client.client;

import java.security.Permission;

/**
 * Client request to add a distributed object listener to a remote node.
 */
public class RemoveDistributedObjectListenerRequest extends BaseClientRemoveListenerRequest {

    public RemoveDistributedObjectListenerRequest() {
    }

    public RemoveDistributedObjectListenerRequest(String registrationId) {
        super(null, registrationId);
    }

    @Override
    public Object call() throws Exception {
        return clientEngine.getProxyService().removeProxyListener(registrationId);
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.REMOVE_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
