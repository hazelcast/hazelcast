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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.client.client.BaseClientRemoveListenerRequest;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import java.security.Permission;

public class RemoveEntryListenerRequest extends BaseClientRemoveListenerRequest {


    public RemoveEntryListenerRequest() {
    }

    public RemoveEntryListenerRequest(String name, String registrationId) {
        super(name, registrationId);
    }

    public Object call() throws Exception {
        final MultiMapService service = getService();
        return service.removeListener(name, registrationId);
    }

    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    public int getClassId() {
        return MultiMapPortableHook.REMOVE_ENTRY_LISTENER;
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "removeEntryListener";
    }

}
