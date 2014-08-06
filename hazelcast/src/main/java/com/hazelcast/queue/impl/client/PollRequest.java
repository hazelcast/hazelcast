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

package com.hazelcast.queue.impl.client;

import com.hazelcast.queue.impl.PollOperation;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Provides the request service for {@link com.hazelcast.queue.impl.PollOperation}
 */
public class PollRequest extends QueueRequest {

    public PollRequest() {
    }

    public PollRequest(String name) {
        super(name);
    }

    public PollRequest(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    @Override
    protected Operation prepareOperation() {
        return new PollOperation(name, timeoutMillis);
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.POLL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        if (timeoutMillis == -1) {
            return "take";
        }
        return "poll";
    }

    @Override
    public Object[] getParameters() {
        if (timeoutMillis > 0) {
            return new Object[]{timeoutMillis, TimeUnit.MILLISECONDS};
        }
        return null;
    }
}
