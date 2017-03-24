/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class HazelcastWriteConnectors {

    private HazelcastWriteConnectors() {
    }

    public static ProcessorMetaSupplier writeMap(String name) {
        return writeMap(name, null);
    }

    public static ProcessorMetaSupplier writeMap(String name, ClientConfig clientConfig) {
        return ProcessorMetaSupplier.of(new BufferedProcessorSupplier<>(
                serializableConfig(clientConfig),
                ArrayMap::new,
                ArrayMap::add,
                instance -> map -> putAllIMap(instance, name, map),
                map -> {
                }
        ));
    }

    public static ProcessorMetaSupplier writeCache(String name) {
        return writeCache(name, null);
    }

    public static ProcessorMetaSupplier writeCache(String name, ClientConfig clientConfig) {
        return ProcessorMetaSupplier.of(new BufferedProcessorSupplier<>(
                serializableConfig(clientConfig),
                ArrayMap::new,
                ArrayMap::add,
                instance -> map -> putAllICache(instance, name, map),
                map -> {
                }
        ));
    }

    public static ProcessorSupplier writeList(String name) {
        return writeList(name, null);
    }

    public static ProcessorSupplier writeList(String name, ClientConfig clientConfig) {
        return new BufferedProcessorSupplier<>(
                serializableConfig(clientConfig),
                ArrayList::new,
                ArrayList::add,
                instance -> list -> addAllIList(instance, name, list),
                list -> {
                }
        );
    }

    private static void putAllIMap(HazelcastInstance instance, String name, ArrayMap map) {
        instance.getMap(name).putAll(map);
        map.clear();
    }

    private static void putAllICache(HazelcastInstance instance, String name, ArrayMap map) {
        instance.getCacheManager().getCache(name).putAll(map);
        map.clear();
    }

    private static void addAllIList(HazelcastInstance instance, String name, ArrayList list) {
        instance.getList(name).addAll(list);
        list.clear();
    }

    private static SerializableClientConfig serializableConfig(ClientConfig clientConfig) {
        return clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
    }

    private static final class ArrayMap extends AbstractMap {

        private final List<Entry> entries;
        private final ArraySet set = new ArraySet();

        ArrayMap() {
            entries = new ArrayList<>();
        }

        @Override @Nonnull
        public Set<Entry> entrySet() {
            return set;
        }

        public void add(Map.Entry entry) {
            entries.add(entry);
        }

        private class ArraySet extends AbstractSet<Entry> {
            @Override @Nonnull
            public Iterator<Entry> iterator() {
                return entries.iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }
        }
    }
}
