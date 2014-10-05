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

import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.queue.impl.operations.IsEmptyOperation;
import com.hazelcast.queue.impl.QueuePortableHook;
import com.hazelcast.spi.Operation;

/**
 * Request to check if the Queue is empty
 */
public class IsEmptyRequest extends QueueRequest implements RetryableRequest {

    public IsEmptyRequest() {
    }

    public IsEmptyRequest(final String name) {
        super(name);
    }

    @Override
    protected Operation prepareOperation() {
        return new IsEmptyOperation(name);
    }

    @Override
    public int getClassId() {
        return QueuePortableHook.IS_EMPTY;
    }

    @Override
    public String getMethodName() {
        return "isEmpty";
    }
}
