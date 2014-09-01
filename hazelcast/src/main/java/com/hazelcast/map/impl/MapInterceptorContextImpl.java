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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.operation.MapOperationType;
import com.hazelcast.nio.serialization.Data;
import java.util.Map;

public class MapInterceptorContextImpl implements MapInterceptorContext {

    private String mapName;
    private MapOperationType operationType;
    private Data key;
    private Object newValue;
    private Map.Entry existingEntry;

    public MapInterceptorContextImpl(String mapName, MapOperationType operationType, Data key, Object newValue,
                                     Map.Entry existingEntry) {
        this.mapName = mapName;
        this.operationType = operationType;
        this.key = key;
        this.newValue = newValue;
        this.existingEntry = existingEntry;
    }

    @Override
    public String getMapName() {
        return mapName;
    }

    public void setNewValue(Object newValue) {
        this.newValue = newValue;
    }

    @Override
    public MapOperationType getOperationType() {
        return operationType;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public Object getNewValue() {
        return newValue;
    }

    @Override
    public Map.Entry getExistingEntry() {
        return existingEntry;
    }
}
