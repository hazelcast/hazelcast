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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

/**
 * Returns if the key loading and dispatching has finished on this partition
 */
public class IsKeyLoadFinishedOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private boolean isFinished;

    public IsKeyLoadFinishedOperation() {
    }

    public IsKeyLoadFinishedOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        isFinished = recordStore.isKeyLoadFinished();
    }

    @Override
    public Object getResponse() {
        return isFinished;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.IS_KEYLOAD_FINISHED;
    }
}
