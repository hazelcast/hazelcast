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

@GenerateCodec(id = TemplateConstants.MULTIMAP_TEMPLATE_ID, name = "MultiMap", ns = "Hazelcast.Client.Protocol.Codec")
public interface MultiMapCodecTemplate {
    /**
     * Stores a key-value pair in the multimap.
     *
     * @param name     Name of the MultiMap
     * @param key      The key to be stored
     * @param value    The value to be stored
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return True if size of the multimap is increased, false if the multimap already contains the key-value pair.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object put(String name, Data key, Data value, long threadId);

    /**
     * Returns the collection of values associated with the key. The collection is NOT backed by the map, so changes to
     * the map are NOT reflected in the collection, and vice-versa.
     *
     * @param name     Name of the MultiMap
     * @param key      The key whose associated values are to be returned
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return The collection of the values associated with the key.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "key")
    Object get(String name, Data key, long threadId);

    /**
     * Removes the given key value pair from the multimap.
     *
     * @param name     Name of the MultiMap
     * @param key      The key of the entry to remove
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return True if the size of the multimap changed after the remove operation, false otherwise.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "key")
    Object remove(String name, Data key, long threadId);

    /**
     * Returns the set of keys in the multimap.The collection is NOT backed by the map, so changes to the map are NOT
     * reflected in the collection, and vice-versa.
     *
     * @param name Name of the MultiMap
     * @return The set of keys in the multimap. The returned set might be modifiable but it has no effect on the multimap.
     */
    @Request(id = 4, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object keySet(String name);

    /**
     * Returns the collection of values in the multimap.The collection is NOT backed by the map, so changes to the map
     * are NOT reflected in the collection, and vice-versa.
     *
     * @param name Name of the MultiMap
     * @return The collection of values in the multimap. the returned collection might be modifiable but it has no effect on the multimap.
     */
    @Request(id = 5, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object values(String name);

    /**
     * Returns the set of key-value pairs in the multimap.The collection is NOT backed by the map, so changes to the map
     * are NOT reflected in the collection, and vice-versa
     *
     * @param name Name of the MultiMap
     * @return The set of key-value pairs in the multimap. The returned set might be modifiable but it has no effect on the multimap.
     */
    @Request(id = 6, retryable = true, response = ResponseMessageConst.LIST_ENTRY)
    Object entrySet(String name);

    /**
     * Returns whether the multimap contains an entry with the key.
     *
     * @param name     Name of the MultiMap
     * @param key      The key whose existence is checked.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
     * @return True if the multimap contains an entry with the key, false otherwise.
     */
    @Request(id = 7, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object containsKey(String name, Data key, long threadId);

    /**
     * Returns whether the multimap contains an entry with the value.
     *
     * @param name  Name of the MultiMap
     * @param value The value whose existence is checked.
     * @return True if the multimap contains an entry with the value, false otherwise.
     */
    @Request(id = 8, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsValue(String name, Data value);

    /**
     * Returns whether the multimap contains the given key-value pair.
     *
     * @param name     Name of the MultiMap
     * @param key      The key whose existence is checked.
     * @param value    The value whose existence is checked.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     * @return True if the multimap contains the key-value pair, false otherwise.
     */
    @Request(id = 9, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object containsEntry(String name, Data key, Data value, long threadId);

    /**
     * Returns the number of key-value pairs in the multimap.
     *
     * @param name Name of the MultiMap
     * @return The number of key-value pairs in the multimap.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     * Clears the multimap. Removes all key-value pairs.
     *
     * @param name Name of the MultiMap
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     * Returns the number of values that match the given key in the multimap.
     *
     * @param name     Name of the MultiMap
     * @param key      The key whose values count is to be returned
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     * @return The number of values that match the given key in the multimap
     */
    @Request(id = 12, retryable = true, response = ResponseMessageConst.INTEGER)
    Object valueCount(String name, Data key, long threadId);

    /**
     * Adds the specified entry listener for the specified key.The listener will be notified for all
     * add/remove/update/evict events for the specified key only.
     *
     * @param name         Name of the MultiMap
     * @param key          The key to listen to
     * @param includeValue True if EntryEvent should contain the value,false otherwise
     * @param localOnly    if true fires events that originated from this node only, otherwise fires all events
     * @return Returns registration id for the entry listener
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerToKey(String name, Data key, boolean includeValue, boolean localOnly);

    /**
     * Adds an entry listener for this multimap. The listener will be notified for all multimap add/remove/update/evict events.
     *
     * @param name         Name of the MultiMap
     * @param includeValue True if EntryEvent should contain the value,false otherwise
     * @param localOnly    if true fires events that originated from this node only, otherwise fires all events
     * @return Returns registration id for the entry listener
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListener(String name, boolean includeValue, boolean localOnly);

    /**
     * Removes the specified entry listener. Returns silently if no such listener was added before.
     *
     * @param name           Name of the MultiMap
     * @param registrationId Registration id of listener
     * @return True if registration is removed, false otherwise
     */
    @Request(id = 15, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     * Acquires the lock for the specified key for the specified lease time. After the lease time, the lock will be
     * released. If the lock is not available, then the current thread becomes disabled for thread scheduling
     * purposes and lies dormant until the lock has been acquired. Scope of the lock is for this map only. The acquired
     * lock is only for the key in this map.Locks are re-entrant, so if the key is locked N times, then it should be
     * unlocked N times before another thread can acquire it.
     *
     * @param name     Name of the MultiMap
     * @param key      The key the Lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     * @param ttl      The duration in milliseconds after which this entry shall be deleted. O means infinite.
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void lock(String name, Data key, long threadId, long ttl);

    /**
     * Tries to acquire the lock for the specified key for the specified lease time. After lease time, the lock will be
     * released. If the lock is not available, then the current thread becomes disabled for thread scheduling purposes
     * and lies dormant until one of two things happens:the lock is acquired by the current thread, or the specified
     * waiting time elapses.
     *
     * @param name     Name of the MultiMap
     * @param key      Key to lock in this map.
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     * @param lease    Time in milliseconds to wait before releasing the lock.
     * @param timeout  Maximum time to wait for the lock.
     * @return True if the lock was acquired and false if the waiting time elapsed before the lock acquired
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object tryLock(String name, Data key, long threadId, long lease, long timeout);

    /**
     * Checks the lock for the specified key. If the lock is acquired, this method returns true, else it returns false.
     *
     * @param name Name of the MultiMap
     * @param key  Key to lock to be checked.
     * @return True if the lock acquired,false otherwise
     */
    @Request(id = 18, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object isLocked(String name, Data key);

    /**
     * Releases the lock for the specified key regardless of the lock owner. It always successfully unlocks the key,
     * never blocks and returns immediately.
     *
     * @param name     Name of the MultiMap
     * @param key      The key to Lock
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void unlock(String name, Data key, long threadId);

    /**
     * Releases the lock for the specified key regardless of the lock owner. It always successfully unlocks the key,
     * never blocks and returns immediately.
     *
     * @param name Name of the MultiMap
     * @param key  The key to Lock
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "key")
    void forceUnlock(String name, Data key);

    /**
     * Removes all the entries with the given key. The collection is NOT backed by the map, so changes to the map are
     * NOT reflected in the collection, and vice-versa.
     *
     * @param name     Name of the MultiMap
     * @param key      The key of the entry to remove
     * @param value    The value of the entry to remove
     * @param threadId The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation
     * @return True if the size of the multimap changed after the remove operation, false otherwise.
     */
    @Request(id = 21, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object removeEntry(String name, Data key, Data value, long threadId);
}

