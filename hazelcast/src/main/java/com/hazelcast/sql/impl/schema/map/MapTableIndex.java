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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.IndexType;

import java.util.List;

/**
 * Definition of an index of the IMap or ReplicatedMap.
 */
public class MapTableIndex {

    private final String name;
    private final IndexType type;

    /** Ordinals of the fields in the owning table. */
    private final List<Integer> fieldOrdinals;

    public MapTableIndex(String name, IndexType type, List<Integer> fieldOrdinals) {
        this.name = name;
        this.type = type;
        this.fieldOrdinals = fieldOrdinals;
    }

    public String getName() {
        return name;
    }

    public IndexType getType() {
        return type;
    }

    public List<Integer> getFieldOrdinals() {
        return fieldOrdinals;
    }
}
