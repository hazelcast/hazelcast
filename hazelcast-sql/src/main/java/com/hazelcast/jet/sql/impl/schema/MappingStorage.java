/*
 * Copyright 2021 Hazelcast Inc.
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

import com.hazelcast.core.EntryListener;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.Mapping;

import java.util.Collection;
import java.util.stream.Collectors;

public class MappingStorage extends AbstractMetadataStorage<Mapping> {

    private static final String CATALOG_MAP_NAME = "__sql.catalog";

    public MappingStorage(NodeEngine nodeEngine) {
        super(nodeEngine, CATALOG_MAP_NAME);
    }

    Collection<Mapping> values() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof Mapping)
                .map(m -> (Mapping) m)
                .collect(Collectors.toList());
    }

    void registerListener(EntryListener<String, Object> listener) {
        // do not try to implicitly create ReplicatedMap
        // TODO: perform this check in a single place i.e. SqlService ?
        if (!nodeEngine.getLocalMember().isLiteMember()) {
            storage().addEntryListener(listener);
        }
    }
}
