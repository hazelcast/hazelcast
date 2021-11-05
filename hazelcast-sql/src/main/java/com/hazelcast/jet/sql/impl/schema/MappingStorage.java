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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.MapEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.Mapping;

import java.util.Collection;

public class MappingStorage extends AbstractMetadataStorage<Mapping> {

    private static final String CATALOG_MAP_NAME = "__sql.catalog";

    public MappingStorage(NodeEngine nodeEngine) {
       super(nodeEngine, CATALOG_MAP_NAME);
    }

    Collection<Mapping> values() {
        return storage().values();
    }

    void registerListener(EntryListener<String, Mapping> listener) {
        // do not try to implicitly create ReplicatedMap
        // TODO: perform this check in a single place i.e. SqlService ?
        if (!nodeEngine.getLocalMember().isLiteMember()) {
            storage().addEntryListener(listener);
        }
    }

    abstract static class EntryListenerAdapter implements EntryListener<String, Mapping> {

        @Override
        public final void entryAdded(EntryEvent<String, Mapping> event) {
        }

        @Override
        public abstract void entryUpdated(EntryEvent<String, Mapping> event);

        @Override
        public abstract void entryRemoved(EntryEvent<String, Mapping> event);

        @Override
        public final void entryEvicted(EntryEvent<String, Mapping> event) {
            throw new UnsupportedOperationException("SQL catalog entries must never be evicted - " + event);
        }

        @Override
        public void entryExpired(EntryEvent<String, Mapping> event) {
            throw new UnsupportedOperationException("SQL catalog entries must never be expired - " + event);
        }

        @Override
        public final void mapCleared(MapEvent event) {
            throw new UnsupportedOperationException("SQL catalog must never be cleared - " + event);
        }

        @Override
        public final void mapEvicted(MapEvent event) {
            throw new UnsupportedOperationException("SQL catalog must never be evicted - " + event);
        }
    }
}
