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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

import java.util.List;
import java.util.Map;

@GenerateCodec(id = TemplateConstants.MAP_TEMPLATE_ID, name = "Map", ns = "Hazelcast.Client.Protocol.Codec")
public interface MapCodecTemplate {
    /**
     * Puts an entry into this map with a given ttl (time to live) value.Entry will expire and get evicted after the ttl
     * If ttl is 0, then the entry lives forever.This method returns a clone of the previous value, not the original
     * (identically equal) value previously put into the map.Time resolution for TTL is seconds. The given TTL value is
     * rounded to the next closest second value.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    Value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     * @return old value of the entry
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object put(String name, Data key, Data value, long threadId, long ttl);

    /**
     * This method returns a clone of the original value, so modifying the returned value does not change the actual
     * value in the map. You should put the modified value back to make changes visible to all nodes.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The value for the key if exists
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object get(String name, Data key, long threadId);

    /**
     * Removes the mapping for a key from this map if it is present (optional operation).
     * Returns the value to which this map previously associated the key, or null if the map contained no mapping for the key.
     * If this map permits null values, then a return value of null does not necessarily indicate that the map contained no mapping for the key; it's also
     * possible that the map explicitly mapped the key to null. The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Clone of the removed value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object remove(String name, Data key, long threadId);

    /**
     * Replaces the entry for a key only if currently mapped to a given value.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object replace(String name, Data key, Data value, long threadId);

    /**
     * Replaces the the entry for a key only if existing values equal to the testValue
     *
     * @param name      Name of the map.
     * @param key       Key for the map entry.
     * @param testValue Test the existing value against this value to find if equal to this value.
     * @param value     New value for the map entry. Only replace with this value if existing value is equal to the testValue.
     * @param threadId  The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return true if value is replaced with new one, false otherwise
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object replaceIfSame(String name, Data key, Data testValue, Data value, long threadId);

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Returns true if the key exists, otherwise returns false.
     */
    @Request(id = 9, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object containsKey(String name, Data key, long threadId);

    /**
     * Returns true if this map maps one or more keys to the specified value.This operation will probably require time
     * linear in the map size for most implementations of the Map interface.
     *
     * @param name  Name of the map.
     * @param value Value to check if exists in the map.
     * @return Returns true if the value exists, otherwise returns false.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsValue(String name, Data value);

    /**
     * Removes the mapping for a key from this map if existing value equal to the this value
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    Test the existing value against this value to find if equal to this value. Only remove the entry from the map if the value is equal to this value.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return Returns true if the key exists and removed, otherwise returns false.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object removeIfSame(String name, Data key, Data value, long threadId);

    /**
     * Removes the mapping for a key from this map if it is present.Unlike remove(Object), this operation does not return
     * the removed value, which avoids the serialization cost of the returned value.If the removed value will not be used,
     * a delete operation is preferred over a remove operation for better performance. The map will not contain a mapping
     * for the specified key once the call returns.
     * This method breaks the contract of EntryListener. When an entry is removed by delete(), it fires an EntryEvent
     * with a null oldValue. Also, a listener with predicates will have null values, so only keys can be queried via predicates
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void delete(String name, Data key, long threadId);

    /**
     * If this map has a MapStore, this method flushes all the local dirty entries by calling MapStore.storeAll()
     * and/or MapStore.deleteAll().
     *
     * @param name Name of the map.
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.VOID)
    void flush(String name);

    /**
     * Tries to remove the entry with the given key from this map within the specified timeout value.
     * If the key is already locked by another thread and/or member, then this operation will wait the timeout
     * amount for acquiring the lock.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout  maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object tryRemove(String name, Data key, long threadId, long timeout);

    /**
     * Tries to put the given key and value into this map within a specified timeout value. If this method returns false,
     * it means that the caller thread could not acquire the lock for the key within the timeout duration,
     * thus the put operation is not successful.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param timeout  maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 15, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object tryPut(String name, Data key, Data value, long threadId, long timeout);

    /**
     * Same as put except that MapStore, if defined, will not be called to store/persist the entry.
     * If ttl is 0, then the entry lives forever.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void putTransient(String name, Data key, Data value, long threadId, long ttl);

    /**
     * Puts an entry into this map with a given ttl (time to live) value if the specified key is not already associated
     * with a value. Entry will expire and get evicted after the ttl.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     * @return returns a clone of the previous value, not the original (identically equal) value previously put into the map.
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object putIfAbsent(String name, Data key, Data value, long threadId, long ttl);

    /**
     * Puts an entry into this map with a given ttl (time to live) value.Entry will expire and get evicted after the ttl
     * If ttl is 0, then the entry lives forever. Similar to the put operation except that set doesn't
     * return the old value, which is more efficient.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param value    New value for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void set(String name, Data key, Data value, long threadId, long ttl);

    /**
     * Acquires the lock for the specified lease time.After lease time, lock will be released.If the lock is not
     * available then the current thread becomes disabled for thread scheduling purposes and lies dormant until the lock
     * has been acquired.
     * Scope of the lock is this map only. Acquired lock is only for the key in this map. Locks are re-entrant,
     * so if the key is locked N times then it should be unlocked N times before another thread can acquire it.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void lock(String name, Data key, long threadId, long ttl);

    /**
     * Tries to acquire the lock for the specified key for the specified lease time.After lease time, the lock will be
     * released.If the lock is not available, then the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until one of two things happens the lock is acquired by the current thread, or
     * the specified waiting time elapses.
     *
     * @param name     Name of the map.
     * @param key      Key for the map entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @param lease    time in milliseconds to wait before releasing the lock.
     * @param timeout  maximum time to wait for getting the lock.
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object tryLock(String name, Data key, long threadId, long lease, long timeout);

    /**
     * Checks the lock for the specified key.If the lock is acquired then returns true, else returns false.
     *
     * @param name name of map
     * @param key  Key for the map entry to check if it is locked.
     * @return Returns true if the entry is locked, otherwise returns false
     */
    @Request(id = 21, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object isLocked(String name, Data key);

    /**
     * Releases the lock for the specified key. It never blocks and returns immediately.
     * If the current thread is the holder of this lock, then the hold count is decremented.If the hold count is zero,
     * then the lock is released.  If the current thread is not the holder of this lock,
     * then ILLEGAL_MONITOR_STATE is thrown.
     *
     * @param name     name of map
     * @param key      Key for the map entry to unlock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     */
    @Request(id = 22, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void unlock(String name, Data key, long threadId);

    /**
     * Adds an interceptor for this map. Added interceptor will intercept operations
     * and execute user defined methods and will cancel operations if user defined method throw exception.
     *
     * @param name        name of map
     * @param interceptor interceptor to add
     * @return id of registered interceptor.
     */
    @Request(id = 23, retryable = false, response = ResponseMessageConst.STRING)
    Object addInterceptor(String name, Data interceptor);

    /**
     * Removes the given interceptor for this map so it will not intercept operations anymore.
     *
     * @param name name of map
     * @param id   of interceptor
     * @return Returns true if successful, otherwise returns false
     */
    @Request(id = 24, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeInterceptor(String name, String id);

    /**
     * Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
     * sub-interface for that event.
     *
     * @param name          name of map
     * @param key           Key for the map entry.
     * @param predicate     predicate for filtering entries.
     * @param includeValue  true if EntryEvent should
     *                      contain the value.
     * @param listenerFlags flags of enabled listeners.
     * @param localOnly     if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string which is used as a key to remove the listener.
     */
    @Request(id = 25, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate,
                                              boolean includeValue, int listenerFlags, boolean localOnly);

    /**
     * Adds an continuous entry listener for this map. Listener will get notified for map add/remove/update/evict events
     * filtered by the given predicate.
     *
     * @param name          name of map
     * @param predicate     predicate for filtering entries.
     * @param includeValue  true if EntryEvent should
     *                      contain the value.
     * @param listenerFlags flags of enabled listeners.
     * @param localOnly     if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string which is used as a key to remove the listener.
     */
    @Request(id = 26, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerWithPredicate(String name, Data predicate, boolean includeValue,
                                         int listenerFlags, boolean localOnly);

    /**
     * Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
     * sub-interface for that event.
     *
     * @param name          name of map
     * @param key           Key for the map entry.
     * @param includeValue  true if EntryEvent should contain the value.
     * @param listenerFlags flags of enabled listeners.
     * @param localOnly     if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string which is used as a key to remove the listener.
     */
    @Request(id = 27, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListenerToKey(String name, Data key, boolean includeValue, int listenerFlags, boolean localOnly);

    /**
     * Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
     * sub-interface for that event.
     *
     * @param name          name of map
     * @param includeValue  true if EntryEvent should contain the value.
     * @param listenerFlags flags of enabled listeners.
     * @param localOnly     if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string which is used as a key to remove the listener.
     */
    @Request(id = 28, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addEntryListener(String name, boolean includeValue, int listenerFlags, boolean localOnly);

    /**
     * Adds an entry listener for this map. Listener will get notified for all map add/remove/update/evict events.
     *
     * @param name          name of map
     * @param listenerFlags flags of enabled listeners.
     * @param localOnly     if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string which is used as a key to remove the listener.
     */
    @Request(id = 29, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_IMAPINVALIDATION, EventMessageConst.EVENT_IMAPBATCHINVALIDATION})
    Object addNearCacheEntryListener(String name, int listenerFlags, boolean localOnly);

    /**
     * Removes the specified entry listener. Returns silently if there is no such listener added before.
     *
     * @param name           name of map
     * @param registrationId id of registered listener.
     * @return true if registration is removed, false otherwise.
     */
    @Request(id = 30, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     * Adds a MapPartitionLostListener. The addPartitionLostListener returns a register-id. This id is needed to remove
     * the MapPartitionLostListener using the removePartitionLostListener(String) method.
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * IMPORTANT: Please see com.hazelcast.partition.PartitionLostListener for weaknesses.
     * IMPORTANT: Listeners registered from HazelcastClient may miss some of the map partition lost events due
     * to design limitations.
     *
     * @param name      name of map
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return returns the registration id for the MapPartitionLostListener.
     */
    @Request(id = 31, retryable = false, response = ResponseMessageConst.STRING,
            event = EventMessageConst.EVENT_MAPPARTITIONLOST)
    Object addPartitionLostListener(String name, boolean localOnly);

    /**
     * Removes the specified map partition lost listener. Returns silently if there is no such listener added before.
     *
     * @param name           name of map
     * @param registrationId id of register
     * @return true if registration is removed, false otherwise.
     */
    @Request(id = 32, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removePartitionLostListener(String name, String registrationId);

    /**
     * Returns the EntryView for the specified key.
     * This method returns a clone of original mapping, modifying the returned value does not change the actual value
     * in the map. One should put modified value back to make changes visible to all nodes.
     *
     * @param name     name of map
     * @param key      the key of the entry.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return EntryView of the specified key.
     */
    @Request(id = 33, retryable = true, response = ResponseMessageConst.ENTRY_VIEW, partitionIdentifier = "key")
    Object getEntryView(String name, Data key, long threadId);

    /**
     * Evicts the specified key from this map. If a MapStore is defined for this map, then the entry is not deleted
     * from the underlying MapStore, evict only removes the entry from the memory.
     *
     * @param name     name of map
     * @param key      the specified key to evict from this map.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return true if the key is evicted, false otherwise.
     */
    @Request(id = 34, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object evict(String name, Data key, long threadId);

    /**
     * Evicts all keys from this map except the locked ones. If a MapStore is defined for this map, deleteAll is not
     * called by this method. If you do want to deleteAll to be called use the clear method. The EVICT_ALL event is
     * fired for any registered listeners.
     *
     * @param name name of map
     */
    @Request(id = 35, retryable = false, response = ResponseMessageConst.VOID)
    void evictAll(String name);

    /**
     * Loads all keys into the store. This is a batch load operation so that an implementation can optimize the multiple loads.
     *
     * @param name                  name of map
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will
     *                              be replaced by those loaded from the MapLoader
     */
    @Request(id = 36, retryable = false, response = ResponseMessageConst.VOID)
    void loadAll(String name, boolean replaceExistingValues);

    /**
     * Loads the given keys. This is a batch load operation so that an implementation can optimize the multiple loads.
     *
     * @param name                  name of map
     * @param keys                  keys to load
     * @param replaceExistingValues when <code>true</code>, existing values in the Map will be replaced by those loaded from the MapLoader
     */
    @Request(id = 37, retryable = false, response = ResponseMessageConst.VOID)
    void loadGivenKeys(String name, List<Data> keys, boolean replaceExistingValues);

    /**
     * Returns a set clone of the keys contained in this map. The set is NOT backed by the map, so changes to the map
     * are NOT reflected in the set, and vice-versa. This method is always executed by a distributed query, so it may
     * throw a QueryResultSizeExceededException if query result size limit is configured.
     *
     * @param name name of the map
     * @return a set clone of the keys contained in this map.
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 38, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object keySet(String name);

    /**
     * Returns the entries for the given keys. If any keys are not present in the Map, it will call loadAll The returned
     * map is NOT backed by the original map, so changes to the original map are NOT reflected in the returned map, and vice-versa.
     * Please note that all the keys in the request should belong to the partition id to which this request is being sent, all keys
     * matching to a different partition id shall be ignored. The API implementation using this request may need to send multiple
     * of these request messages for filling a request for a key set if the keys belong to different partitions.
     *
     * @param name name of map
     * @param keys keys to get
     * @return values for the provided keys.
     */
    @Request(id = 39, retryable = false, response = ResponseMessageConst.LIST_ENTRY, partitionIdentifier = "any key belongs to target partition")
    Object getAll(String name, List<Data> keys);

    /**
     * Returns a collection clone of the values contained in this map.
     * The collection is NOT backed by the map, so changes to the map are NOT reflected in the collection, and vice-versa.
     * This method is always executed by a distributed query, so it may throw a QueryResultSizeExceededException
     * if query result size limit is configured.
     *
     * @param name name of map
     * @return All values in the map
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 40, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object values(String name);

    /**
     * Returns a Set clone of the mappings contained in this map.
     * The collection is NOT backed by the map, so changes to the map are NOT reflected in the collection, and vice-versa.
     * This method is always executed by a distributed query, so it may throw a QueryResultSizeExceededException
     * if query result size limit is configured.
     *
     * @param name name of map
     * @return a set clone of the keys mappings in this map
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 41, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object entrySet(String name);

    /**
     * Queries the map based on the specified predicate and returns the keys of matching entries. Specified predicate
     * runs on all members in parallel.The set is NOT backed by the map, so changes to the map are NOT reflected in the
     * set, and vice-versa. This method is always executed by a distributed query, so it may throw a
     * QueryResultSizeExceededException if query result size limit is configured.
     *
     * @param name      name of map.
     * @param predicate specified query criteria.
     * @return result key set for the query.
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 42, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object keySetWithPredicate(String name, Data predicate);

    /**
     * Queries the map based on the specified predicate and returns the values of matching entries.Specified predicate
     * runs on all members in parallel. The collection is NOT backed by the map, so changes to the map are NOT reflected
     * in the collection, and vice-versa. This method is always executed by a distributed query, so it may throw a
     * QueryResultSizeExceededException if query result size limit is configured.
     *
     * @param name      name of map
     * @param predicate specified query criteria.
     * @return result value collection of the query.
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 43, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object valuesWithPredicate(String name, Data predicate);

    /**
     * Queries the map based on the specified predicate and returns the matching entries.Specified predicate
     * runs on all members in parallel. The collection is NOT backed by the map, so changes to the map are NOT reflected
     * in the collection, and vice-versa. This method is always executed by a distributed query, so it may throw a
     * QueryResultSizeExceededException if query result size limit is configured.
     *
     * @param name      name of map
     * @param predicate specified query criteria.
     * @return result key-value entry collection of the query.
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 44, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object entriesWithPredicate(String name, Data predicate);

    /**
     * Adds an index to this map for the specified entries so that queries can run faster.If you are querying your values
     * mostly based on age and active then you should consider indexing these fields.
     * Index attribute should either have a getter method or be public.You should also make sure to add the indexes before
     * adding entries to this map.
     * Indexing time is executed in parallel on each partition by operation threads. The Map is not blocked during this
     * operation.The time taken in proportional to the size of the Map and the number Members.
     * Until the index finishes being created, any searches for the attribute will use a full Map scan, thus avoiding
     * using a partially built index and returning incorrect results.
     *
     * @param name      name of map
     * @param attribute index attribute of value
     * @param ordered   true if index should be ordered, false otherwise.
     */
    @Request(id = 45, retryable = false, response = ResponseMessageConst.VOID)
    void addIndex(String name, String attribute, boolean ordered);

    /**
     * Returns the number of key-value mappings in this map.  If the map contains more than Integer.MAX_VALUE elements,
     * returns Integer.MAX_VALUE
     *
     * @param name of map
     * @return the number of key-value mappings in this map
     */
    @Request(id = 46, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     * Returns true if this map contains no key-value mappings.
     *
     * @param name name of map
     * @return true if this map contains no key-value mappings
     */
    @Request(id = 47, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

    /**
     * Copies all of the mappings from the specified map to this map (optional operation).The effect of this call is
     * equivalent to that of calling put(Object,Object) put(k, v) on this map once for each mapping from key k to value
     * v in the specified map.The behavior of this operation is undefined if the specified map is modified while the
     * operation is in progress.
     * Please note that all the keys in the request should belong to the partition id to which this request is being sent, all keys
     * matching to a different partition id shall be ignored. The API implementation using this request may need to send multiple
     * of these request messages for filling a request for a key set if the keys belong to different partitions.
     *
     * @param name    name of map
     * @param entries mappings to be stored in this map
     */
    @Request(id = 48, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "any key belongs to target partition")
    void putAll(String name, Map<Data, Data> entries);

    /**
     * This method clears the map and invokes MapStore#deleteAll deleteAll on MapStore which, if connected to a database,
     * will delete the records from that database. The MAP_CLEARED event is fired for any registered listeners.
     * To clear a map without calling MapStore#deleteAll, use #evictAll.
     *
     * @param name of map
     */
    @Request(id = 49, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key. Returns the the object which is result of
     * the process() method of EntryProcessor.
     *
     * @param name           name of map
     * @param entryProcessor processor to execute on the map entry
     * @param key            the key of the map entry.
     * @return result of entry process.
     */
    @Request(id = 50, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object executeOnKey(String name, Data entryProcessor, Data key, long threadId);

    /**
     * Applies the user defined EntryProcessor to the entry mapped by the key. Returns immediately with a Future
     * representing that task.EntryProcessor is not cancellable, so calling Future.cancel() method won't cancel the
     * operation of EntryProcessor.
     *
     * @param name           name of map
     * @param entryProcessor entry processor to be executed on the entry.
     * @param key            the key of the map entry.
     * @return result of entry process.
     */
    @Request(id = 51, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object submitToKey(String name, Data entryProcessor, Data key, long threadId);

    /**
     * Applies the user defined EntryProcessor to the all entries in the map.Returns the results mapped by each key in the map.
     *
     * @param name           name of map
     * @param entryProcessor entry processor to be executed.
     * @return results of entry process on the entries
     */
    @Request(id = 52, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object executeOnAllKeys(String name, Data entryProcessor);

    /**
     * Applies the user defined EntryProcessor to the entries in the map which satisfies provided predicate.
     * Returns the results mapped by each key in the map.
     *
     * @param name           name of map
     * @param entryProcessor entry processor to be executed.
     * @param predicate      specified query criteria.
     * @return results of entry process on the entries matching the query criteria
     */
    @Request(id = 53, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object executeWithPredicate(String name, Data entryProcessor, Data predicate);

    /**
     * Applies the user defined EntryProcessor to the entries mapped by the collection of keys.The results mapped by
     * each key in the collection.
     *
     * @param name           name of map
     * @param entryProcessor entry processor to be executed.
     * @param keys           The keys for the entries for which the entry processor shall be executed on.
     * @return results of entry process on the entries with the provided keys
     */
    @Request(id = 54, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object executeOnKeys(String name, Data entryProcessor, List<Data> keys);

    /**
     * Releases the lock for the specified key regardless of the lock owner.It always successfully unlocks the key,
     * never blocks,and returns immediately.
     *
     * @param name name of map
     * @param key  the key of the map entry.
     */
    @Request(id = 55, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void forceUnlock(String name, Data key);

    /**
     * @param name      name of map
     * @param predicate specified query criteria.
     * @return result keys for the query.
     */
    @Request(id = 56, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object keySetWithPagingPredicate(String name, Data predicate);

    /**
     * Queries the map based on the specified predicate and returns the values of matching entries. Specified predicate
     * runs on all members in parallel. The collection is NOT backed by the map, so changes to the map are NOT reflected
     * in the collection, and vice-versa. This method is always executed by a distributed query, so it may throw a
     * QueryResultSizeExceededException if query result size limit is configured.
     *
     * @param name      name of map
     * @param predicate specified query criteria.
     * @return values for the query.
     * @see com.hazelcast.instance.GroupProperty#QUERY_RESULT_SIZE_LIMIT
     */
    @Request(id = 57, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object valuesWithPagingPredicate(String name, Data predicate);

    /**
     * @param name      name of map
     * @param predicate specified query criteria.
     * @return key-value pairs for the query.
     */
    @Request(id = 58, retryable = false, response = ResponseMessageConst.LIST_ENTRY)
    Object entriesWithPagingPredicate(String name, Data predicate);

    @Request(id = 59, retryable = false, response = ResponseMessageConst.VOID)
    void clearNearCache(String name, Address target);
}
