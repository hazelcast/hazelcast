/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.atomicreference.AtomicReferenceContainer;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import static com.hazelcast.concurrent.atomicreference.AtomicReferenceDataSerializerHook.GET_AND_ALTER;

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
        AtomicReferenceContainer container = getReferenceContainer();

        response = container.get();
        Object input = nodeEngine.toObject(container.get());
        //noinspection unchecked
        Object output = f.apply(input);
        Data serializedOutput = nodeEngine.toData(output);
        shouldBackup = !isEquals(response, serializedOutput);
        if (shouldBackup) {
            container.set(serializedOutput);
            backup = serializedOutput;
        }
    }

    @Override
    public int getId() {
        return GET_AND_ALTER;
    }
}
