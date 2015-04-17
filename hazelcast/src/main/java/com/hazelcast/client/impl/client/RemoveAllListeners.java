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

package com.hazelcast.client.impl.client;

import java.security.Permission;

/**
 * When a connection does not respond to heart-beat we switch the listeners to another endpoint
 * If somehow connection starts to respond heart-beat we need to signal the endpoint to remove the listeners
 * An internal request should not be used for user calls.
 */
public class RemoveAllListeners extends CallableClientRequest {

    public RemoveAllListeners() {
    }

    @Override
    public Object call() throws Exception {
        endpoint.clearAllListeners();
        return null;
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
        return ClientPortableHook.REMOVE_ALL_LISTENERS;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
