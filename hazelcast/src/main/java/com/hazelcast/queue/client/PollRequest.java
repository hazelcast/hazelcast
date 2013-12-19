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

package com.hazelcast.queue.client;

import com.hazelcast.queue.PollOperation;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

/**
 * @author ali 5/8/13
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

    protected Operation prepareOperation() {
        return new PollOperation(name, timeoutMillis);
    }

    public int getClassId() {
        return QueuePortableHook.POLL;
    }

    public Permission getRequiredPermission() {
        return new QueuePermission(name, ActionConstants.ACTION_REMOVE);
    }
}
