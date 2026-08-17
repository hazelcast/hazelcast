/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A mapping of keys to {@link VectorDocument}s.
 *
 * <p>
 * <b>Important note</b>
 * <p>
 * Key-based operations such as {@link #getAsync(Object)} or {@link #putAsync(Object, VectorDocument)}
 * do not use {@code hashCode} and {@code equals} implementations of key's class.
 * Instead, they use {@code hashCode} and {@code equals} of the serialized form of the key.
 * <p>
 * Each mutating operation requires that no optimization process is currently being performed on any index.
 * If any index is undergoing an optimization process,
 * the mutable method will throw an exception {@link IndexMutationDisallowedException}.
 * If an index is already undergoing an optimization process,
 * attempting to start a second optimization process on the same index
 * will result an exception {@link IndexMutationDisallowedException}.
 * Multiple optimization processes can be performed simultaneously on different indexes.
 *
 * @param <K> key type.
 * @param <V> value type.
 * @since 5.5
 */
@Beta
public interface VectorCollection<K, V> extends DistributedObject {

    /**
     * Asynchronously gets the {@link VectorDocument} associated with the given key or {@code null} if no such association exists.
     *
     * @param key the key of the entry
     * @return the {@link VectorDocument} associated with the given key or {@code null} if no such association exists.
     */
    CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key);

    /**
     * Asynchronously associates the given key with the {@code value} {@link VectorDocument},
     * returning the {@link VectorDocument} previously associated with the {@code key} if such an association existed.
     *
     * @param key the key of the entry
     * @param value the value to associate the entry with
     * @return  the {@link VectorDocument} previously associated with {@code key} or {@code null} if
     *          no {@link VectorDocument} was previously associated with {@code key}.
     */
    CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> value);

    /**
     * Asynchronously associates the given key with the {@code value} {@link VectorDocument}.
     *
     * @param key the key of the entry
     * @param value the value to associate the entry with
     */
    CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> value);

    /**
     * If the given {@code key} is not already associated with a {@link VectorDocument},
     * associates it with the given {@code value} and returns {@code null}, else returns the current value.
     *
     * @param key the key of the entry
     * @param value the value to associate the entry with
     * @return  the previous {@link VectorDocument} associated with {@code key} or {@code null} if no such association existed.
     */
    CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> value);

    /**
     * Inserts asynchronously the given map of key-document pairs.
     *
     * @param documents a {@link Map} of key-document associations to insert in the {@code VectorCollection}.
     */
    CompletionStage<Void> putAllAsync(Map<? extends K, VectorDocument<V>> documents);

    /**
     * Removes asynchronously the association of given {@code key} to a document, if such an association existed,
     * returning the {@link VectorDocument} previously associated with the {@code key} if such an association existed.
     *
     * @param key the key to remove from the {@code VectorCollection}
     * @return  the {@link VectorDocument} previously associated with the given {@code key}
     *          or {@code null} if no such association existed.
     */
    CompletionStage<VectorDocument<V>> removeAsync(K key);

    /**
     * Removes asynchronously the association of given {@code key} to a document, if such an association existed.
     *
     * @param key the key to remove from the {@code VectorCollection}
     */
    CompletionStage<Void> deleteAsync(K key);

    /**
     * Optimize the specified index by fully removing nodes marked for deletion,
     * trimming neighbor sets to the advertised degree, and updating the entry node as necessary.
     * <p>
     * Backups of this operation are always executed as async backups.
     *
     * @param indexName the name of the index to be optimized or null for the only index
     *
     * @return a {@link CompletionStage} with a void value if the process finishes successfully;
     * or completed exceptionally with an {@link IndexMutationDisallowedException}
     * if the index is currently undergoing an optimization operation;
     * or completed exceptionally with an {@link IllegalArgumentException} if index does not exist.
     */
    CompletionStage<Void> optimizeAsync(@Nullable String indexName);

    /**
     * Optimize the only index by fully removing nodes marked for deletion,
     * trimming neighbor sets to the advertised degree, and updating the entry node as necessary.
     * <p>
     * Backups of this operation are always executed as async backups.
     *
     * @return a {@link CompletionStage} with a void value if the process finishes successfully;
     * or completed exceptionally with an {@link IndexMutationDisallowedException}
     * if the index is currently undergoing an optimization operation;
     * or completed exceptionally with an {@link IllegalArgumentException} if the collection has more than one index.
     */
    default CompletionStage<Void> optimizeAsync() {
        return optimizeAsync(null);
    }

    /**
     * Asynchronously clears all entries in the vector collection.
     * <p>
     * Backups of this operation are always executed as async backups.
     *
     * @return A {@link CompletionStage} that completes when the clear operation is finished.
     */
    CompletionStage<Void> clearAsync();

    /**
     * Returns the number of values in the vector collection.
     *
     * @return the number of values in the vector collection as a long.
     */
    long size();

    /**
     * Perform asynchronously a similarity search according to the options in given {@code searchOptions}.
     * <p>
     * If there are many concurrent modifications during search, it is possible but extremely unlikely
     * to receive fewer results than requested, even when the collection contains enough items.
     *
     * @param vectors the search vector. Can be unnamed if the collection has only one index,
     *                otherwise it has to be associated with index name
     * @param searchOptions the search options
     * @return {@code SearchResults} object that allows to iterate over search results in order of descending similarity score
     */
    CompletionStage<SearchResults<K, V>> searchAsync(VectorValues vectors, SearchOptions searchOptions);

    /**
     * Creates or retrieves a {@link VectorCollection} for the given configuration.
     *
     * <p>This is a legacy convenience method that first registers the provided
     * {@link VectorCollectionConfig} on the {@link HazelcastInstance} and then
     * retrieves the corresponding vector collection by name.</p>
     *
     * @param instance the Hazelcast instance
     * @param collectionConfig configuration of the vector collection
     * @param <K> key type
     * @param <V> value type
     * @return the vector collection instance
     * @deprecated since 6.0 use {@link HazelcastInstance#getVectorCollection(String)}
     */
    @Deprecated(since = "6.0")
    static <K, V> VectorCollection<K, V> getCollection(HazelcastInstance instance,
                                                       VectorCollectionConfig collectionConfig) {
        String name = collectionConfig.getName();
        instance.getConfig().addVectorCollectionConfig(collectionConfig);
        return getCollection(instance, name);
    }

    /**
     * Returns an existing {@link VectorCollection} instance with the specified name.
     * <p>
     * @param instance the Hazelcast instance
     * @param name the name of the vector collection
     * @param <K> key type
     * @param <V> value type
     * @return the vector collection instance
     * @deprecated since 6.0 use {@link HazelcastInstance#getVectorCollection(String)}
     */
    @Deprecated(since = "6.0")
    static <K, V> VectorCollection<K, V> getCollection(HazelcastInstance instance, String name) {
        return instance.getVectorCollection(name);
    }
}
