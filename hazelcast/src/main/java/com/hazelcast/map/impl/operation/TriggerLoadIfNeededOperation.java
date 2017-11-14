/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.RunStatus;

import static com.hazelcast.spi.RunStatus.HAS_RESPONSE;

/**
 * Triggers key loading on member with {@link com.hazelcast.map.impl.MapKeyLoader.Role#SENDER}
 * or {@link com.hazelcast.map.impl.MapKeyLoader.Role#SENDER_BACKUP} key
 * loader role if keys have not yet been loaded.
 * <p>
 * Returns the previous state of the key loading and dispatching future.
 */
public class TriggerLoadIfNeededOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Boolean isLoaded;

    public TriggerLoadIfNeededOperation() {
    }

    public TriggerLoadIfNeededOperation(String name) {
        super(name);
    }

    @Override
    public void run() {
        isLoaded = recordStore.isKeyLoadFinished();
        recordStore.maybeDoInitialLoad();
    }

    @Override
    public Object getResponse() {
        return isLoaded;
    }

    public RunStatus runStatus() {
        return HAS_RESPONSE;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.TRIGGER_LOAD_IF_NEEDED;
    }

}
