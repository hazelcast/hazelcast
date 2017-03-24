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

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.Consumer;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

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
import static com.hazelcast.jet.impl.util.Util.noopConsumer;
import static java.util.stream.Collectors.toList;

public final class HazelcastWriters {

    private HazelcastWriters() {
    }

    @Nonnull
    public static ProcessorSupplier writeMap(String name) {
        return writeMap(name, null);
    }

    @Nonnull
    public static ProcessorSupplier writeMap(String name, ClientConfig clientConfig) {
        return new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                ArrayMap::new,
                ArrayMap::add,
                instance -> {
                    IMap map = instance.getMap(name);
                    return buffer -> {
                        map.putAll(buffer);
                        buffer.clear();
                    };
                },
                noopConsumer()
        );
    }

    @Nonnull
    public static ProcessorSupplier writeCache(String name) {
        return writeCache(name, null);
    }

    @Nonnull
    public static ProcessorSupplier writeCache(String name, ClientConfig clientConfig) {
        return new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                ArrayMap::new,
                ArrayMap::add,
                instance -> {
                    ICache cache = instance.getCacheManager().getCache(name);
                    return buffer -> {
                        cache.putAll(buffer);
                        buffer.clear();
                    };
                },
                noopConsumer()
        );
    }

    @Nonnull
    public static ProcessorSupplier writeList(String name) {
        return writeList(name, null);
    }

    @Nonnull
    public static ProcessorSupplier writeList(String name, ClientConfig clientConfig) {
        return new HazelcastWriterSupplier<>(
                serializableConfig(clientConfig),
                ArrayList::new,
                ArrayList::add,
                instance -> {
                    IList<Object> list = instance.getList(name);
                    return buffer -> {
                        list.addAll(buffer);
                        buffer.clear();
                    };
                },
                noopConsumer()
        );
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

    private static class HazelcastWriterSupplier<B, T> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig clientConfig;
        private final Function<HazelcastInstance, Consumer<B>> instanceToFlushBuffer;
        private final Supplier<B> bufferSupplier;
        private final BiConsumer<B, T> addToBuffer;
        private final Consumer<B> disposeBuffer;

        private transient Consumer<B> flushBuffer;
        private transient HazelcastInstance client;

        HazelcastWriterSupplier(SerializableClientConfig clientConfig,
                                Supplier<B> newBuffer,
                                BiConsumer<B, T> addToBuffer,
                                Function<HazelcastInstance, Consumer<B>> instanceToFlushBuffer,
                                Consumer<B> disposeBuffer) {
            this.clientConfig = clientConfig;
            this.instanceToFlushBuffer = instanceToFlushBuffer;
            this.bufferSupplier = newBuffer;
            this.addToBuffer = addToBuffer;
            this.disposeBuffer = disposeBuffer;
        }

        @Override
        public void init(@Nonnull Context context) {
            HazelcastInstance instance;
            if (isRemote()) {
                instance = client = newHazelcastClient(clientConfig.asClientConfig());
            } else {
                instance = context.jetInstance().getHazelcastInstance();
            }
            flushBuffer = instanceToFlushBuffer.apply(instance);
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
            return Stream.generate(() -> new WriteBufferedP<>(bufferSupplier, addToBuffer, flushBuffer, disposeBuffer))
                         .limit(count).collect(toList());
        }
    }
}
