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

import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockPortableHook;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;

import java.security.Permission;

/**
 * @author mdogan 5/3/13
 */
public final class LockRequest extends AbstractLockRequest {

    public LockRequest() {
    }

    public LockRequest(Data key, int threadId) {
        super(key, threadId);
    }

    public LockRequest(Data key, int threadId, long ttl, long timeout) {
        super(key, threadId, ttl, timeout);
    }

    protected InternalLockNamespace getNamespace() {
        String name = (String) getClientEngine().toObject(key);
        return new InternalLockNamespace(name);
    }

    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    public int getClassId() {
        return LockPortableHook.LOCK;
    }

    public Permission getRequiredPermission() {
        String name = (String) getClientEngine().toObject(key);
        return new LockPermission(name, ActionConstants.ACTION_LOCK);
    }

}
