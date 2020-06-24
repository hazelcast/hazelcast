/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.sql.impl.type.QueryDataType;

// TODO: can it be non-thread safe ?
public class JsonUpsertTarget implements UpsertTarget {

    private JsonObject json;

    JsonUpsertTarget() {
    }

    @Override
    public UpsertInjector createInjector(String path) {
        // TODO: unquoted support for booleans & numbers ???
        return value -> json.add(path, (String) QueryDataType.VARCHAR.convert(value));
    }

    @Override
    public void init() {
        json = Json.object();
    }

    @Override
    public Object conclude() {
        JsonObject json = this.json;
        this.json = null;
        return new HazelcastJsonValue(json.toString());
    }
}
