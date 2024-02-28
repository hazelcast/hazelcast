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
import com.hazelcast.sql.impl.schema.SqlCatalogObject;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.view.View;

import java.util.Collection;
import java.util.stream.Collectors;

public class RelationsStorage extends AbstractRelationsStorage {
    public RelationsStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    void put(String name, Mapping mapping) {
        storage().put(name, mapping);
    }

    @Override
    void put(String name, View view) {
        storage().put(name, view);
    }

    @Override
    void put(String name, Type type) {
        storage().put(name, type);
    }

    @Override
    boolean putIfAbsent(String name, Mapping mapping) {
        return storage().putIfAbsent(name, mapping) == null;
    }

    @Override
    boolean putIfAbsent(String name, View view) {
        return storage().putIfAbsent(name, view) == null;
    }

    @Override
    boolean putIfAbsent(String name, Type type) {
        return storage().putIfAbsent(name, type) == null;
    }

    @Override
    public Mapping getMapping(String name) {
        Object object = storage().get(name);
        return object instanceof Mapping mapping ? mapping : null;
    }

    @Override
    public Collection<Type> getAllTypes() {
        return storage().values().stream()
                .filter(Type.class::isInstance)
                .map(o -> (Type) o)
                .collect(Collectors.toList());
    }

    @Override
    public Type getType(final String name) {
        Object object = storage().get(name);
        return object instanceof Type type ? type : null;
    }

    @Override
    Mapping removeMapping(String name) {
        return (Mapping) storage().remove(name);
    }

    @Override
    public Type removeType(String name) {
        return (Type) storage().remove(name);
    }

    @Override
    View removeView(String name) {
        return (View) storage().remove(name);
    }

    @Override
    Collection<String> mappingNames() {
        return objectNames(Mapping.class);
    }

    @Override
    Collection<String> viewNames() {
        return objectNames(View.class);
    }

    @Override
    Collection<String> typeNames() {
        return objectNames(Type.class);
    }

    private Collection<String> objectNames(Class<? extends SqlCatalogObject> clazz) {
        return storage().values()
                .stream()
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .map(SqlCatalogObject::name)
                .collect(Collectors.toList());
    }
}
