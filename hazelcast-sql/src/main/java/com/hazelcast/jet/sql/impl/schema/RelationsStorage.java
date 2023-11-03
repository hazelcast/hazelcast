/*
 * Copyright 2023 Hazelcast Inc.
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
import java.util.stream.Collectors;

public class RelationsStorage extends AbstractSchemaStorage {

    public RelationsStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    void put(String name, Mapping mapping) {
        storage().put(name, mapping);
    }

    void put(String name, View view) {
        storage().put(name, view);
    }

    void put(String name, Type type) {
        storage().put(name, type);
    }

    boolean putIfAbsent(String name, Mapping mapping) {
        return storage().putIfAbsent(name, mapping) == null;
    }

    boolean putIfAbsent(String name, View view) {
        return storage().putIfAbsent(name, view) == null;
    }

    boolean putIfAbsent(String name, Type type) {
        return storage().putIfAbsent(name, type) == null;
    }

    Mapping removeMapping(String name) {
        return (Mapping) storage().remove(name);
    }

    public Mapping getMapping(String name) {
        Object object = storage().get(name);
        return object instanceof Mapping ? (Mapping) object : null;
    }

    public Collection<Type> getAllTypes() {
        return storage().values().stream()
                .filter(o -> o instanceof Type)
                .map(o -> (Type) o)
                .collect(Collectors.toList());
    }

    public Type getType(final String name) {
        Object obj = storage().get(name);
        if (obj instanceof Type) {
            return (Type) obj;
        }
        return null;
    }

    public Type removeType(String name) {
        return (Type) storage().remove(name);
    }

    View removeView(String name) {
        return (View) storage().remove(name);
    }

    Collection<String> mappingNames() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof Mapping)
                .map(m -> ((Mapping) m).name())
                .collect(Collectors.toList());
    }

    Collection<String> viewNames() {
        return storage().values()
                .stream()
                .filter(v -> v instanceof View)
                .map(v -> ((View) v).name())
                .collect(Collectors.toList());
    }

    Collection<String> typeNames() {
        return storage().values()
                .stream()
                .filter(t -> t instanceof Type)
                .map(t -> ((Type) t).name())
                .collect(Collectors.toList());
    }
}
