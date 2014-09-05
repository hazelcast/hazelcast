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

package com.hazelcast.concurrent.atomicreference.client;

import com.hazelcast.concurrent.atomicreference.operations.GetAndSetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class GetAndSetRequest extends ModifyRequest {

    public GetAndSetRequest() {
    }

    public GetAndSetRequest(String name, Data update) {
        super(name, update);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAndSetOperation(name, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.GET_AND_SET;
    }

    @Override
    public String getMethodName() {
        return "getAndSet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{update};
    }
}
