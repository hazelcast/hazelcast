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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public final class GetOperation extends KeyBasedMapOperation implements IdentifiedDataSerializable {

    private transient Data result;

    public GetOperation() {
    }

    public GetOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() {
        result = mapService.toData(recordStore.get(dataKey));
    }

    public void afterRun() {
        mapService.interceptAfterGet(name, result);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public String toString() {
        return "GetOperation{}";
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.GET;
    }
}
