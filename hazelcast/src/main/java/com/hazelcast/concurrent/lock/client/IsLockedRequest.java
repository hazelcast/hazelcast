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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.RetryableRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;

import java.security.Permission;

public final class IsLockedRequest extends AbstractIsLockedRequest
        implements RetryableRequest {

    public IsLockedRequest() {
    }

    public IsLockedRequest(Data key) {
        super(key);
    }

    public IsLockedRequest(Data key, long threadId) {
        super(key, threadId);
    }

    @Override
    protected InternalLockNamespace getNamespace() {
        String name = getName();
        return new InternalLockNamespace(name);
    }

    @Override
    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return LockPortableHook.IS_LOCKED;
    }

    @Override
    public Permission getRequiredPermission() {
        String name = getName();
        return new LockPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getMethodName() {
        if (threadId != 0) {
            return "isLockedByCurrentThread";
        }
        return super.getMethodName();
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
