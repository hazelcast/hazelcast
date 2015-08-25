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
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;
import java.util.Set;

@GenerateCodec(id = TemplateConstants.MAP_TEMPLATE_ID, name = "Map", ns = "Hazelcast.Client.Protocol.Map")
public interface MapCodecTemplate {
    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value Value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     * @return old value of the entry
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    Object put(String name, Data key, Data value, long threadId, long ttl);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The value for the key if exists
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.DATA)
    Object get(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA)
    Object remove(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA)
    Object replace(String name, Data key, Data value, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param testValue Test the existing value against this value to find if equal to this value.
     * @param value New value for the map entry. Only replace with this value if existing value is equal to the testValue.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object replaceIfSame(String name, Data key, Data testValue, Data value, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     * @return Future from which the old value of the key can be retrieved.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    Object putAsync(String name, Data key, Data value, long threadId, long ttl);


    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Future from which the value of the key can be retrieved.
     */
    @Request(id = 7, retryable = true, response = ResponseMessageConst.DATA)
    Object getAsync(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return A Future from which the value removed from the map can be retrieved.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.DATA)
    Object removeAsync(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Returns true if the key exists, otherwise returns false.
     */
    @Request(id = 9, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsKey(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param value Value to check if exists in the map.
     * @return Returns true if the value exists, otherwise returns false.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsValue(String name, Data value);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value Test the existing value against this value to find if equal to this value. Only remove the entry from the map if the value is equal to this value.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Returns true if the key exists and removed, otherwise returns false.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeIfSame(String name, Data key, Data value, long threadId);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID)
    void delete(String name, Data key, long threadId);

    /**
     *
     * @param name Name of the map.
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.VOID)
    void flush(String name);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object tryRemove(String name, Data key, long threadId, long timeout);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 15, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object tryPut(String name, Data key, Data value, long threadId, long timeout);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID)
    void putTransient(String name, Data key, Data value, long threadId, long ttl);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     * @return returns a clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.DATA)
    Object putIfAbsent(String name, Data key, Data value, long threadId, long ttl);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param value New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.VOID)
    void set(String name, Data key, Data value, long threadId, long ttl);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID)
    void lock(String name, Data key, long threadId, long ttl);

    /**
     *
     * @param name Name of the map.
     * @param key Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lease time in milliseconds to wait before releasing the lock.
     * @param timeout maximum time to wait for getting the lock.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object tryLock(String name, Data key, long threadId, long lease, long timeout);

    /**
     *
     * @param name name of map
     * @param key Key for the map entry to check if it is locked.
     * @return Returns true if the entry is locked, otherwise returns false
     */
    @Request(id = 21, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isLocked(String name, Data key);

    /**
     *
     * @param name name of map
     * @param key Key for the map entry to unlock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 22, retryable = false, response = ResponseMessageConst.VOID)
    void unlock(String name, Data key, long threadId);

    /**
     *
     * @param name name of map
     * @param interceptor interceptor to add
     * @return id of registered interceptor.
     */
    @Request(id = 23, retryable = false, response = ResponseMessageConst.STRING)
    Object addInterceptor(String name, Data interceptor);

    /**
     *
     * @param name name of map
     * @param id of interceptor
     * @return Returns true if successful, otherwise returns false
     *
     */
    @Request(id = 24, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeInterceptor(String name, String id);

    /**
     *
     * @param name name of map
     * @param key Key for the map entry.
     * @param predicate    predicate for filtering entries.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     */
    @Request(id = 25, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate, boolean includeValue);

    /**
     *
     * @param name name of map
     * @param predicate    predicate for filtering entries.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     *                     contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.

     */
    @Request(id = 26, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerWithPredicate(String name, Data predicate, boolean includeValue);

    /**
     *
     * @param name name of map
     * @param key Key for the map entry.
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     */
    @Request(id = 27, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerToKey(String name, Data key, boolean includeValue);

    /**
     *
     * @param name name of map
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should contain the value.
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     */
    @Request(id = 28, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListener(String name, boolean includeValue);

    /**
     *
     * @param name name of map
     * @param includeValue <tt>true</tt> if <tt>EntryEvent</tt> should
     * @return A UUID.randomUUID().toString() which is used as a key to remove the listener.
     */
    @Request(id = 29, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addNearCacheEntryListener(String name, boolean includeValue);

    /**
     *
     * @param name name of map
     * @param registrationId id of registered listener.
     * @return true if registration is removed, false otherwise.
     */
    @Request(id = 30, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     *
     * @param name name of map
     * @return returns the registration id for the MapPartitionLostListener.
     */
    @Request(id = 31, retryable = true, response = ResponseMessageConst.STRING,
            event = EventMessageConst.EVENT_MAPPARTITIONLOST)
    Object addPartitionLostListener(String name);

    /**
     *
     * @param name name of map
     * @param registrationId id of register
     * @return true if registration is removed, false otherwise.
     */
    @Request(id = 32, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removePartitionLostListener(String name, String registrationId);

    /**
     *
     * @param name name of map
     * @param key the key of the entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return <tt>EntryView</tt> of the specified key.
     */
    @Request(id = 33, retryable = true, response = ResponseMessageConst.ENTRY_VIEW)
    Object getEntryView(String name, Data key, long threadId);

    /**
     *
     * @param name name of map
     * @param key the specified key to evict from this map.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return <tt>true</tt> if the key is evicted, <tt>false</tt> otherwise.
     */
    @Request(id = 34, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object evict(String name, Data key, long threadId);

    /**
     *
     * @param name name of map
     */
    @Request(id = 35, retryable = false, response = ResponseMessageConst.VOID)
    void evictAll(String name);

    /**
     *
     * @param name name of map
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     */
    @Request(id = 36, retryable = false, response = ResponseMessageConst.VOID)
    void loadAll(String name, boolean replaceExistingValues);

    /**
     *
     * @param name name of map
     * @param keys keys to load
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will be replaced by those loaded from the MapLoader
     */
    @Request(id = 37, retryable = false, response = ResponseMessageConst.VOID)
    void loadGivenKeys(String name, Set<Data> keys, boolean replaceExistingValues);

    /**
     *
     * @param name name of the map
     * @return a set clone of the keys contained in this map.
     */
    @Request(id = 38, retryable = false, response = ResponseMessageConst.SET_DATA)
    Object keySet(String name);

    /**
     *
     * @param name name of map
     * @param keys keys to get
     * @return values for the provided keys.
     */
    @Request(id = 39, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object getAll(String name, Set<Data> keys);

    /**
     *
     * @param name name of map
     * @return All values in the map
     */
    @Request(id = 40, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object values(String name);

    /**
     *
     * @param name name of map
     * @return a set clone of the keys mappings in this map
     */
    @Request(id = 41, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object entrySet(String name);

    /**
     *
     * @param name name of map.
     * @param predicate specified query criteria.
     * @return result key set for the query.
     */
    @Request(id = 42, retryable = false, response = ResponseMessageConst.SET_DATA)
    Object keySetWithPredicate(String name, Data predicate);

    /**
     *
     * @param name name of map
     * @param predicate specified query criteria.
     * @return result value collection of the query.
     */
    @Request(id = 43, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object valuesWithPredicate(String name, Data predicate);

    /**
     *
     * @param name name of map
     * @param predicate specified query criteria.
     * @return result key-value entry collection of the query.
     */
    @Request(id = 44, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object entriesWithPredicate(String name, Data predicate);

    /**
     *
     * @param name name of map
     * @param attribute index attribute of value
     * @param ordered   <tt>true</tt> if index should be ordered, <tt>false</tt> otherwise.
     */
    @Request(id = 45, retryable = false, response = ResponseMessageConst.VOID)
    void addIndex(String name, String attribute, boolean ordered);

    /**
     *
     * @param name of map
     * @return the number of key-value mappings in this map
     */
    @Request(id = 46, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     *
     * @param name name of map
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    @Request(id = 47, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

    /**
     *
     * @param name name of map
     * @param entries mappings to be stored in this map
     *
     */
    @Request(id = 48, retryable = false, response = ResponseMessageConst.VOID)
    void putAll(String name, Map<Data, Data> entries);

    /**
     *
     * @param name of map
     */
    @Request(id = 49, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     *
     * @param name name of map
     * @param entryProcessor processor to execute on the map entry
     * @param key the key of the map entry.
     * @return result of entry process.
     */
    @Request(id = 50, retryable = false, response = ResponseMessageConst.DATA)
    Object executeOnKey(String name, Data entryProcessor, Data key, long threadId);

    /**
     *
     * @param name name of map
     * @param entryProcessor entry processor to be executed on the entry.
     * @param key the key of the map entry.
     * @return Future from which the result of the operation can be retrieved.
     */
    @Request(id = 51, retryable = false, response = ResponseMessageConst.DATA)
    Object submitToKey(String name, Data entryProcessor, Data key, long threadId);

    /**
     *
     * @param name name of map
     * @param entryProcessor entry processor to be executed.
     * @return results of entry process on the entries
     */
    @Request(id = 52, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object executeOnAllKeys(String name, Data entryProcessor);

    /**
     *
     * @param name name of map
     * @param entryProcessor entry processor to be executed.
     * @param predicate specified query criteria.
     * @return results of entry process on the entries matching the query criteria
     */
    @Request(id = 53, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object executeWithPredicate(String name, Data entryProcessor, Data predicate);

    /**
     *
     * @param name name of map
     * @param entryProcessor entry processor to be executed.
     * @param keys The keys for the entries for which the entry processor shall be executed on.
     * @return results of entry process on the entries with the provided keys
     *
     */
    @Request(id = 54, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object executeOnKeys(String name, Data entryProcessor, Set<Data> keys);

    /**
     *
     * @param name name of map
     * @param key the key of the map entry.
     *
     */
    @Request(id = 55, retryable = false, response = ResponseMessageConst.VOID)
    void forceUnlock(String name, Data key);

    /**
     *
     * @param name name of map
     * @param predicate specified query criteria.
     * @return result keys for the query.
     *
     */
    @Request(id = 56, retryable = false, response = ResponseMessageConst.SET_DATA)
    Object keySetWithPagingPredicate(String name, Data predicate);

    /**
     *
     * @param name name of map
     * @param predicate specified query criteria.
     * @return values for the query.
     */
    @Request(id = 57, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object valuesWithPagingPredicate(String name, Data predicate);

    /**
     *
     * @param name name of map
     * @param predicate specified query criteria.
     * @return key-value pairs for the query.
     *
     */
    @Request(id = 58, retryable = false, response = ResponseMessageConst.SET_ENTRY)
    Object entriesWithPagingPredicate(String name, Data predicate);

}
