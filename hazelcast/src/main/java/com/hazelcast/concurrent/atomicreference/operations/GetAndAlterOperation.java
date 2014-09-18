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

package com.hazelcast.concurrent.atomicreference.operations;

import com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.concurrent.atomicreference.ReferenceWrapper;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

public class GetAndAlterOperation extends AbstractAlterOperation {

    public GetAndAlterOperation() {
    }

    public GetAndAlterOperation(String name, Data function) {
        super(name, function);
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        IFunction f = nodeEngine.toObject(function);
        ReferenceWrapper reference = getReference();

        Object input = nodeEngine.toObject(reference.get());
        response = input;
        //noinspection unchecked
        Object output = f.apply(input);
        shouldBackup = !isEquals(input, output);
        if (shouldBackup) {
            backup = nodeEngine.toData(output);
            reference.set(backup);
        }
    }

    @Override
    public int getId() {
        return AtomicReferenceDataSerializerHook.GET_AND_ALTER;
    }
}
