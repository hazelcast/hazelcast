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

import com.hazelcast.concurrent.atomicreference.operations.SetOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

public class SetRequest extends ModifyRequest {

    public SetRequest() {
    }

    public SetRequest(String name, Data update) {
        super(name, update);
    }

    @Override
    protected Operation prepareOperation() {
        return new SetOperation(name, update);
    }

    @Override
    public int getClassId() {
        return AtomicReferencePortableHook.SET;
    }

    @Override
    public String getMethodName() {
        if (update == null) {
            return "clear";
        }
        return "set";
    }

    @Override
    public Object[] getParameters() {
        if (update == null) {
            return null;
        }
        return new Object[]{update};
    }
}
