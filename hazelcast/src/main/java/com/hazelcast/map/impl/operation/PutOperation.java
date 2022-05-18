/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.Immutable;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.util.Arrays;

import static com.hazelcast.map.impl.record.Record.UNSET;

public class PutOperation extends BasePutOperation implements MutatingOperation {

    public PutOperation() {
    }

    public PutOperation(String name, Data dataKey, Object value) {
        super(name, dataKey, value);
    }

    @Override
    protected void runInternal() {

        if (mapContainer.getMapConfig().getInMemoryFormat() == InMemoryFormat.OBJECT) {

            if (dataValue instanceof Data) {
                Object dataObject = mapServiceContext.toObject(dataValue);
                if (dataObject instanceof Immutable
                    || Arrays.stream(dataObject.getClass().getInterfaces())
                        .anyMatch(c -> c.getName().equals("java.lang.constant.Constable"))) {
                    oldValue = recordStore.put(dataKey, dataObject, getTtl(), getMaxIdle());
                } else {
                    oldValue = recordStore.put(dataKey, dataValue, getTtl(), getMaxIdle());
                }
            } else {
                if (dataValue instanceof Immutable
                    || Arrays.stream(dataValue.getClass().getInterfaces())
                        .anyMatch(c -> c.getName().equals("java.lang.constant.Constable"))) {
                    oldValue = recordStore.put(dataKey, dataValue, getTtl(), getMaxIdle());
                } else {
                    oldValue = recordStore.put(dataKey, mapServiceContext.toData(dataValue), getTtl(), getMaxIdle());
                }
            }
            // TODO: If !Immutable - do a defensive copy
        } else {
            oldValue = recordStore.put(dataKey, mapServiceContext.toData(dataValue), getTtl(), getMaxIdle());
        }
    }

    // overridden in extension classes
    protected long getTtl() {
        return UNSET;
    }

    // overridden in extension classes
    protected long getMaxIdle() {
        return UNSET;
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT;
    }
}
