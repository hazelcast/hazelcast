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

package com.hazelcast.topic.impl.client;

import com.hazelcast.client.client.BaseClientRemoveListenerRequest;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.impl.TopicPortableHook;
import com.hazelcast.topic.impl.TopicService;

import java.security.Permission;

public class RemoveMessageListenerRequest extends BaseClientRemoveListenerRequest {

    public RemoveMessageListenerRequest() {
    }

    public RemoveMessageListenerRequest(String name, String registrationId) {
        super(name, registrationId);
    }

    @Override
    public Boolean call() throws Exception {
        TopicService service = getService();
        return service.removeMessageListener(name, registrationId);
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicPortableHook.REMOVE_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "removeMessageListener";
    }
}
