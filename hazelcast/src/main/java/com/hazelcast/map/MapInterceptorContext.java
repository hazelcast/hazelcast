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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;

import java.util.Map;

public class MapInterceptorContext {

    private String mapName;
    private MapOperationType operationType;
    private Data key;
    private Object newValue;
    private Map.Entry existingEntry;

    public MapInterceptorContext(String mapName, MapOperationType operationType, Data key, Object newValue, Map.Entry existingEntry) {
        this.mapName = mapName;
        this.operationType = operationType;
        this.key = key;
        this.newValue = newValue;
        this.existingEntry = existingEntry;
    }

    public String getMapName() {
        return mapName;
    }

    public void setNewValue(Object newValue) {
        this.newValue = newValue;
    }

    public MapOperationType getOperationType() {
        return operationType;
    }

    public Data getKey() {
        return key;
    }

    public Object getNewValue() {
        return newValue;
    }

    public Map.Entry getExistingEntry() {
        return existingEntry;
    }
}
