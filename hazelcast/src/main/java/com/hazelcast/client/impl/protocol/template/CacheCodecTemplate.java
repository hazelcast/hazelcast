/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Nullable;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Set;

@GenerateCodec(id = TemplateConstants.JCACHE_TEMPLATE_ID, name = "Cache", ns = "Hazelcast.Client.Protocol.Codec")
public interface CacheCodecTemplate {

    /**
     *
     * @param name Name of the cache.
     * @return Registration id for the registered listener.
     */
    @Request(id = 1, retryable = true, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_CACHE})
    Object addEntryListener(String name);

    /**
     *
     * @param name Name of the cache.
     * @return Registration id for the registered listener.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_CACHEINVALIDATION, EventMessageConst.EVENT_CACHEBATCHINVALIDATION})
    Object addInvalidationListener(String name);

    /**
     * Clears the contents of the cache, without notifying listeners or CacheWriters.
     *
     * @param name Name of the cache.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     * Removes entries for the specified keys. The order in which the individual entries are removed is undefined.
     * For every entry in the key set, the following are called: any registered CacheEntryRemovedListeners if the cache
     * is a write-through cache, the CacheWriter. If the key set is empty, the CacheWriter is not called.
     *
     * @param name Name of the cache.
     * @param keys The keys to remove.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.VOID)
    void removeAllKeys(String name, Set<Data> keys, int completionId);

    /**
     * Removes all of the mappings from this cache. The order that the individual entries are removed is undefined.
     * For every mapping that exists the following are called: any registered CacheEntryRemovedListener if the cache is
     * a write-through cache, the CacheWriter.If the cache is empty, the CacheWriter is not called.
     * This is potentially an expensive operation as listeners are invoked. Use  #clear() to avoid this.
     *
     * @param name Name of the cache.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.VOID)
    void removeAll(String name, int completionId);

    /**
     * Determines if the Cache contains an entry for the specified key. More formally, returns true if and only if this
     * cache contains a mapping for a key k such that key.equals(k). (There can be at most one such mapping.)
     *
     * @param name Name of the cache.
     * @param key The key whose presence in this cache is to be tested.
     * @return Returns true if cache value for the key exists, false otherwise.
     */
    @Request(id = 6, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsKey(String name, Data key);

    /**
     *
     * @param cacheConfig The cache configuration. Byte-array which is serialized from an object implementing
     *                    javax.cache.configuration.Configuration interface.
     * @param createAlsoOnOthers True if the configuration shall be created on all members, false otherwise.
     * @return The created configuration object. Byte-array which is serialized from an object implementing
     *                    javax.cache.configuration.Configuration interface.
     */
    @Request(id = 7, retryable = true, response = ResponseMessageConst.DATA)
    Object createConfig(Data cacheConfig, boolean createAlsoOnOthers);

    /**
     * Closes the cache. Clears the internal content and releases any resource.
     *
     * @param name Name of the cache.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.VOID)
    void destroy(String name);

    /**
     *
     * @param name Name of the cache.
     * @param key            the key to the entry
     * @param entryProcessor Entry processor to invoke. Byte-array which is serialized from an object implementing
     *                       javax.cache.processor.EntryProcessor.
     * @param arguments      additional arguments to pass to the EntryProcessor
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return the result of the processing, if any, defined by the EntryProcessor implementation
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.DATA)
    Object entryProcessor(String name, Data key, Data entryProcessor, List<Data> arguments, int completionId);

    /**
     * Gets a collection of entries from the cache with custom expiry policy, returning them as Map of the values
     * associated with the set of keys requested. If the cache is configured for read-through operation mode, the underlying
     * configured javax.cache.integration.CacheLoader might be called to retrieve the values of the keys from any kind
     * of external resource.
     *
     * @param name Name of the cache.
     * @param keys The keys whose associated values are to be returned.
     * @param expiryPolicy Expiry policy for the entry. Byte-array which is serialized from an object implementing
     *                     javax.cache.expiry.ExpiryPolicy interface.
     * @return A map of entries that were found for the given keys. Keys not found
     *         in the cache are not in the returned map.
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object getAll(String name, Set<Data> keys, @Nullable Data expiryPolicy);

    /**
     * Atomically removes the entry for a key only if currently mapped to some value.
     *
     * @param name Name of the cache.
     * @param key key with which the specified value is associated
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return the value if one existed or null if no mapping existed for this key
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.DATA)
    Object getAndRemove(String name, Data key, int completionId);

    /**
     * Atomically replaces the assigned value of the given key by the specified value using a custom
     * javax.cache.expiry.ExpiryPolicy and returns the previously assigned value. If the cache is configured for
     * write-through operation mode, the underlying configured javax.cache.integration.CacheWriter might be called to
     * store the value of the key to any kind of external resource.
     *
     * @param name Name of the cache.
     * @param key   The key whose value is replaced.
     * @param value The new value to be associated with the specified key.
     * @param expiryPolicy Expiry policy for the entry. Byte-array which is serialized from an object implementing
     *                     javax.cache.expiry.ExpiryPolicy interface.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return The old value previously assigned to the given key.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.DATA)
    Object getAndReplace(String name, Data key, Data value, @Nullable Data expiryPolicy, int completionId);

    /**
     *
     * @param name Name of the cache with prefix.
     * @param simpleName Name of the cache without prefix.
     * @return The cache configuration. Byte-array which is serialized from an object implementing
     *         javax.cache.configuration.Configuration interface.
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.DATA)
    Object getConfig(String name, String simpleName);

    /**
     * Retrieves the mapped value of the given key using a custom javax.cache.expiry.ExpiryPolicy. If no mapping exists
     * null is returned. If the cache is configured for read-through operation mode, the underlying configured
     * javax.cache.integration.CacheLoader might be called to retrieve the value of the key from any kind of external resource.
     *
     * @param name Name of the cache.
     * @param key The key whose mapped value is to be returned.
     * @param expiryPolicy Expiry policy for the entry. Byte-array which is serialized from an object implementing
     *                     javax.cache.expiry.ExpiryPolicy interface.
     *
     * @return The value assigned to the given key, or null if not assigned.
     */
    @Request(id = 14, retryable = true, response = ResponseMessageConst.DATA)
    Object get(String name, Data key, @Nullable Data expiryPolicy);

    /**
     * The ordering of iteration over entries is undefined. During iteration, any entries that are a). read will have
     * their appropriate CacheEntryReadListeners notified and b). removed will have their appropriate
     * CacheEntryRemoveListeners notified. java.util.Iterator#next() may return null if the entry is no longer present,
     * has expired or has been evicted.
     *
     * @param name Name of the cache.
     * @param partitionId The partition id which owns this cache store.
     * @param tableIndex The slot number (or index) to start the iterator
     * @param batch The number of items to be batched
     * @return last index processed and list of data
     */
    @Request(id = 15, retryable = false, response = ResponseMessageConst.CACHE_KEY_ITERATOR_RESULT)
    Object iterate(String name, int partitionId, int tableIndex, int batch);

    /**
     *
     * @param name Name of the cache.
     * @param listenerConfig The listener configuration. Byte-array which is serialized from an object implementing
     *                       javax.cache.configuration.CacheEntryListenerConfiguration
     * @param register true if the listener is being registered, false if the listener is being unregistered.
     * @param address The address of the member server for which the listener is being registered for.
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID)
    void listenerRegistration(String name, Data listenerConfig, boolean register, Address address);

    /**
     *
     * @param name Name of the cache.
     * @param keys                  the keys to load
     * @param replaceExistingValues when true existing values in the Cache will
     *                              be replaced by those loaded from a CacheLoader
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.VOID)
    void loadAll(String name, Set<Data> keys, boolean replaceExistingValues);

    /**
     *
     * @param name Name of the cache.
     * @param isStat true if enabling statistics, false if enabling management.
     * @param enabled true if enabled, false to disable.
     * @param address the address of the host to enable.
     */
    @Request(id = 18, retryable = true, response = ResponseMessageConst.VOID)
    void managementConfig(String name, boolean isStat, boolean enabled, Address address);

    /**
     * Associates the specified key with the given value if and only if there is not yet a mapping defined for the
     * specified key. If the cache is configured for write-through operation mode, the underlying configured
     * javax.cache.integration.CacheWriter might be called to store the value of the key to any kind of external resource.
     *
     * @param name Name of the cache.
     * @param key   The key that is associated with the specified value.
     * @param value The value that has the specified key associated with it.
     * @param expiryPolicy The custom expiry policy for this operation.
     *                     A null value is equivalent to put(Object, Object).

     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return true if a value was set, false otherwise.
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object putIfAbsent(String name, Data key, Data value, @Nullable Data expiryPolicy, int completionId);

    /**
     *
     * @param name Name of the cache.
     * @param key   The key that has the specified value associated with it.
     * @param value The value to be associated with the key.
     * @param expiryPolicy Expiry policy for the entry. Byte-array which is serialized from an object implementing
     *                     javax.cache.expiry.ExpiryPolicy interface.

     * @param get boolean flag indicating if the previous value should be retrieved.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return The value previously assigned to the given key, or null if not assigned.
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.DATA)
    Object put(String name, Data key, Data value, @Nullable Data expiryPolicy, boolean get, int completionId);

    /**
     *
     * @param name Name of the cache.
     * @param registrationId The id assigned during the registration for the listener which shall be removed.
     * @return true if the listener is de-registered, false otherwise
     */
    @Request(id = 21, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     *
     * @param name Name of the cache.
     * @param registrationId The id assigned during the registration for the listener which shall be removed.
     * @return true if the listener is de-registered, false otherwise
     */
    @Request(id = 22, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeInvalidationListener(String name, String registrationId);

    /**
     * Atomically removes the mapping for a key only if currently mapped to the given value.
     *
     * @param name Name of the cache.
     * @param key key whose mapping is to be removed from the cache
     * @param currentValue value expected to be associated with the specified key.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return returns false if there was no matching key
     */
    @Request(id = 23, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object remove(String name, Data key, @Nullable Data currentValue, int completionId);

    /**
     * Atomically replaces the currently assigned value for the given key with the specified newValue if and only if the
     * currently assigned value equals the value of oldValue using a custom javax.cache.expiry.ExpiryPolicy
     * If the cache is configured for write-through operation mode, the underlying configured
     * javax.cache.integration.CacheWriter might be called to store the value of the key to any kind of external resource.
     *
     * @param name Name of the cache.
     * @param key  The key whose value is replaced.
     * @param oldValue Old value to match if exists before removing. Null means "don't try to remove"
     * @param newValue The new value to be associated with the specified key.
     * @param expiryPolicy Expiry policy for the entry. Byte-array which is serialized from an object implementing
     *                     javax.cache.expiry.ExpiryPolicy interface.
     * @param completionId User generated id which shall be received as a field of the cache event upon completion of
     *                     the request in the cluster.
     * @return The replaced value.

     */
    @Request(id = 24, retryable = false, response = ResponseMessageConst.DATA)
    Object replace(String name, Data key, @Nullable Data oldValue, Data newValue, @Nullable Data expiryPolicy, int completionId);

    /**
     * Total entry count
     *
     * @param name Name of the cache.
     * @return total entry count
     */
    @Request(id = 25, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     * Adds a CachePartitionLostListener. The addPartitionLostListener returns a registration ID. This ID is needed to remove the
     * CachePartitionLostListener using the #removePartitionLostListener(String) method. There is no check for duplicate
     * registrations, so if you register the listener twice, it will get events twice.Listeners registered from
     * HazelcastClient may miss some of the cache partition lost events due to design limitations.
     *
     * @param name Name of the cache
     * @return returns the registration id for the CachePartitionLostListener.
     */
    @Request(id = 26, retryable = true, response = ResponseMessageConst.STRING,
            event = EventMessageConst.EVENT_CACHEPARTITIONLOST)
    Object addPartitionLostListener(String name);

    /**
     * Removes the specified cache partition lost listener. Returns silently if there is no such listener added before
     *
     * @param name Name of the Cache
     * @param registrationId ID of registered listener.
     * @return true if registration is removed, false otherwise.
     */

    @Request(id = 27, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removePartitionLostListener(String name, String registrationId);

}
