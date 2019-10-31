/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.config.IndexType;

import java.util.List;

/**
 * Index descriptor.
 */
public class HazelcastTableIndex {
    /** Name. */
    private final String name;

    /** Type. */
    private final IndexType type;

    /** Attributes. */
    private final List<String> attributes;

    public HazelcastTableIndex(String name, IndexType type, List<String> attributes) {
        this.name = name;
        this.type = type;
        this.attributes = attributes;
    }

    public String getName() {
        return name;
    }

    public IndexType getType() {
        return type;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name=" + name + ", type=" + type + ", attributes=" + attributes + '}';
    }
}
