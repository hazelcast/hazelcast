/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;
import com.hazelcast.nio.serialization.Data;

import java.util.AbstractMap;
import java.util.Map;

// todo replace MapEntry with Map.Entry then this class will implement Map.Entry
public class SimpleMapEntry implements MapEntry {

    Map.Entry entry;

    public SimpleMapEntry(Object key, Object value) {
        entry = new AbstractMap.SimpleEntry(key, value);
    }

    public long getCreationTime() {
        return 0;
    }

    public long getLastAccessTime() {
        return 0;
    }

    public Object getKey() {
        return entry.getKey();
    }

    public Object getValue() {
        return entry.getValue();
    }

    public Object setValue(Object value) {
        throw new UnsupportedOperationException();
    }
}
