/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.view.View;

import java.util.Collection;

public abstract class AbstractRelationsStorage extends AbstractSchemaStorage {
    AbstractRelationsStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    abstract void put(String name, Mapping mapping);

    abstract void put(String name, View view);

    abstract void put(String name, Type type);

    abstract boolean putIfAbsent(String name, Mapping mapping);

    abstract boolean putIfAbsent(String name, View view);

    abstract boolean putIfAbsent(String name, Type type);

    public abstract Mapping getMapping(String name);

    public abstract Collection<Type> getAllTypes();

    public abstract Type getType(String name);

    abstract Mapping removeMapping(String name);

    public abstract Type removeType(String name);

    abstract View removeView(String name);

    abstract Collection<String> mappingNames();

    abstract Collection<String> viewNames();

    abstract Collection<String> typeNames();
}
