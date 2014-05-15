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

package com.hazelcast.client;

import java.security.Permission;

/**
 * When a connection does not respond to heart-beat we switch the listeners to another endpoint
 * If somehow connection starts to respond heart-beat we need to signal the endpoint to remove the listeners
 * This class is used for this purpose because of backward-compatibility
 */
public class RemoveDistributedObjectListenerRequest extends BaseClientRemoveListenerRequest {

    public static final String CLEAR_LISTENERS_COMMAND = "clear-all-listeners";

    public RemoveDistributedObjectListenerRequest() {
    }

    public RemoveDistributedObjectListenerRequest(String registrationId) {
        super(null, registrationId);
    }

    @Override
    public Object call() throws Exception {
        //Please see above JavaDoc
        if (CLEAR_LISTENERS_COMMAND.equals(name)) {
            endpoint.clearAllListeners();
            return true;
        }
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
