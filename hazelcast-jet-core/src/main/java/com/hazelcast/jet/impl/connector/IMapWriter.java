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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static java.util.stream.Collectors.toList;

public final class IMapWriter implements Processor {

    private final IMap map;
    private final ArrayMap buffer = new ArrayMap();

    private IMapWriter(IMap map) {
        this.map = map;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drainTo(buffer.entries);
        flush();
    }

    @Override
    public boolean complete() {
        flush();
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void flush() {
        //noinspection unchecked
        map.putAll(buffer);
        buffer.clear();
    }

    public static ProcessorMetaSupplier supplier(String mapName) {
        return new MetaSupplier(mapName);
    }

    public static ProcessorMetaSupplier supplier(String mapName, ClientConfig clientConfig) {
        return new MetaSupplier(mapName, clientConfig);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private SerializableClientConfig clientConfig;

        MetaSupplier(String mapName) {
            this(mapName, null);
        }

        MetaSupplier(String mapName, ClientConfig clientConfig) {
            this.mapName = mapName;
            this.clientConfig = clientConfig != null ? new SerializableClientConfig(clientConfig) : null;
        }

        @Override @Nonnull
        public ProcessorSupplier get(@Nonnull Address address) {
            return new Supplier(mapName, clientConfig);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private SerializableClientConfig clientConfig;
        private transient IMap map;
        private transient HazelcastInstance client;

        Supplier(String name, SerializableClientConfig clientConfig) {
            this.name = name;
            this.clientConfig = clientConfig;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            map = instance.getMap(name);
        }

        @Override
        public void complete(Throwable error) {
            if (client != null) {
                client.shutdown();
            }
        }

        private boolean isRemote() {
            return clientConfig != null;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new IMapWriter(map)).limit(count).collect(toList());
        }
    }

    private static class ArrayMap extends AbstractMap {

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

        private class ArraySet extends AbstractSet<Map.Entry> {
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
