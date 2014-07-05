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

package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.operations.AddAndGetOperation;
import com.hazelcast.spi.Operation;

public class AddAndGetRequest extends AtomicLongRequest {

    public AddAndGetRequest() {
    }

    public AddAndGetRequest(String name, long delta) {
        super(name, delta);
    }

    @Override
    protected Operation prepareOperation() {
        return new AddAndGetOperation(name, delta);
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.ADD_AND_GET;
    }

    @Override
    public String getMethodName() {
        if (delta == -1) {
            return "decrementAndGet";
        } else if (delta == 1) {
            return "incrementAndGet";
        }
        return "addAndGet";
    }

    @Override
    public Object[] getParameters() {
        if (delta == 1 || delta == -1) {
            return null;
        }
        return super.getParameters();
    }
}
