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

package com.hazelcast.connector.map;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Adapter between the source and a Map backed by Hz 3 client
 */
public interface Hz3MapAdapter {

    /**
     * Return number of partitions for the cluster
     */
    int getPartitionCount();

    /**
     * Return reader for a map with given name
     */
    <F extends CompletableFuture, B, R> Reader<F, B, R> reader(String mapName,
                                                               Function<Map.Entry<byte[], byte[]>, Object> toObject);

    /**
     * Returns a {@link BooleanSupplier} that will tell if a migration took
     * place since the call to this method.
     */
    BooleanSupplier createWatcher();

    /**
     * Shutdown the client and any related resources
     */
    void shutdown();

    /**
     * Returns map with given name.
     * <p>
     * The returned type is {@link AsyncMap} because of the incompatible interfaces between Hazelcast 3 and 4.
     */
    <K, V> AsyncMap<K, V> getMap(String mapName);

    /**
     * Returns replicated map with given name
     */
    <V, K> Map<K, V> getReplicatedMap(String mapName);

    /**
     * Wrap payload as Data using Hazelcast 3 version of HeapData
     */
    Object toHz3Data(byte[] bytes);
}
