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

package com.hazelcast.connector.map.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.connector.map.AsyncMap;
import com.hazelcast.connector.map.Hz3MapAdapter;
import com.hazelcast.connector.map.Reader;
import com.hazelcast.core.IMap;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;

/**
 * Implementation of {@link Hz3MapAdapter}
 */
public class MapAdapterImpl implements Hz3MapAdapter {

    private final HazelcastClientProxy client;
    private final MigrationWatcher migrationWatcher;

    /**
     * Create a map adapter from client xml configuration
     *
     * @param clientXml client xml configuration
     */
    public MapAdapterImpl(String clientXml) {
        client = (HazelcastClientProxy) newHazelcastClient(asClientConfig(clientXml));
        migrationWatcher = new MigrationWatcher(client);
    }

    private static ClientConfig asClientConfig(String xml) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        return new XmlClientConfigBuilder(inputStream).build();
    }

    /**
     * Shutdown this adapter (shuts down migration watcher and underlying client)
     */
    @Override
    public void shutdown() {
        if (migrationWatcher != null) {
            migrationWatcher.deregister();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    /**
     * Return number of partitions for the cluster
     */
    @Override
    public int getPartitionCount() {
        return client.client.getClientPartitionService().getPartitionCount();
    }

    @Override
    public <F extends CompletableFuture, B, R> Reader<F, B, R> reader(
            String mapName,
            Function<Map.Entry<byte[], byte[]>, Object> toObject
    ) {
        return (Reader<F, B, R>) new MapReader.RemoteMapReader(client, mapName, toObject);
    }

    @Override
    public BooleanSupplier createWatcher() {
        return migrationWatcher.createWatcher();
    }

    @Override
    public <K, V> AsyncMap<K, V> getMap(String mapName) {
        IMap<K, V> map = client.getMap(mapName);

        return new AsyncMap<K, V>() {

            @Override
            public CompletionStage<V> getAsync(@Nonnull K key) {
                return Hz3ImplUtil.toCompletableFuture(map.getAsync(key));
            }

            @Override
            public int size() {
                return map.size();
            }

            @Override
            public boolean isEmpty() {
                return map.isEmpty();
            }

            @Override
            public boolean containsKey(Object key) {
                return map.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
                return map.containsValue(value);
            }

            @Override
            public V get(Object key) {
                return map.get(key);
            }

            @Override
            public V put(K key, V value) {
                return map.put(key, value);
            }

            @Override
            public V remove(Object key) {
                return map.remove(key);
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                map.putAll(m);
            }

            @Override
            public void clear() {
                map.clear();
            }

            @Override
            public Set<K> keySet() {
                return map.keySet();
            }

            @Override
            public Collection<V> values() {
                return map.values();
            }

            @Override
            public Set<Entry<K, V>> entrySet() {
                return map.entrySet();
            }
        };
    }

    @Override
    public <V, K> Map<K, V> getReplicatedMap(String mapName) {
        return client.getReplicatedMap(mapName);
    }
}
