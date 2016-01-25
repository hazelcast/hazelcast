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

import java.util.List;
import java.util.Map;

@GenerateCodec(id = TemplateConstants.REPLICATED_MAP_TEMPLATE_ID,
        name = "ReplicatedMap", ns = "Hazelcast.Client.Protocol.Codec")
public interface ReplicatedMapCodecTemplate {
    /**
     * Associates a given value to the specified key and replicates it to the cluster. If there is an old value, it will
     * be replaced by the specified one and returned from the call. In addition, you have to specify a ttl and its TimeUnit
     * to define when the value is outdated and thus should be removed from the replicated map.
     *
     * @param name Name of the ReplicatedMap
     * @param key  Key with which the specified value is to be associated.
     * @param value Value to be associated with the specified key
     * @param ttl ttl in milliseconds to be associated with the specified key-value pair
     * @return The old value if existed for the key.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object put(String name, Data key, Data value, long ttl);

    /**
     * Returns the number of key-value mappings in this map. If the map contains more than Integer.MAX_VALUE elements,
     * returns Integer.MAX_VALUE.
     *
     * @param name Name of the ReplicatedMap
     * @return the number of key-value mappings in this map.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "random")
    Object size(String name);

    /**
     * Return true if this map contains no key-value mappings
     *
     * @param name Name of the ReplicatedMap
     * @return <tt>True</tt> if this map contains no key-value mappings
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "random")
    Object isEmpty(String name);

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param name Name of the ReplicatedMap
     * @param key The key whose associated value is to be returned.
     * @return <tt>True</tt> if this map contains a mapping for the specified key
     *
     */
    @Request(id = 4, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "key")
    Object containsKey(String name, Data key);

    /**
     * Returns true if this map maps one or more keys to the specified value.
     * This operation will probably require time linear in the map size for most implementations of the Map interface.
     *
     * @param name Name of the ReplicatedMap
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the specified value
     */
    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "random")
    Object containsValue(String name, Data value);

    /**
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     * If this map permits null values, then a return value of null does not
     * necessarily indicate that the map contains no mapping for the key; it's also possible that the map
     * explicitly maps the key to null.  The #containsKey operation may be used to distinguish these two cases.
     *
     * @param name Name of the ReplicatedMap
     * @param key The key whose associated value is to be returned
     * @return The value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    @Request(id = 6, retryable = true, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object get(String name, Data key);

    /**
     * Removes the mapping for a key from this map if it is present (optional operation). Returns the value to which this map previously associated the key,
     * or null if the map contained no mapping for the key. If this map permits null values, then a return value of
     * null does not necessarily indicate that the map contained no mapping for the key; it's also possible that the map
     * explicitly mapped the key to null. The map will not contain a mapping for the specified key once the call returns.
     *
     * @param name Name of the ReplicatedMap
     * @param key Key with which the specified value is to be associated.
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "key")
    Object remove(String name, Data key);

    /**
     * Copies all of the mappings from the specified map to this map (optional operation). The effect of this call is
     * equivalent to that of calling put(Object,Object) put(k, v) on this map once for each mapping from key k to value
     * v in the specified map. The behavior of this operation is undefined if the specified map is modified while the
     * operation is in progress.
     *
     * @param name Name of the ReplicatedMap
     * @param entries entries to be stored in this map
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.VOID)
    void putAll(String name, List<Map.Entry<Data,Data>> entries);

    /**
     * The clear operation wipes data out of the replicated maps.It is the only synchronous remote operation in this
     * implementation, so be aware that this might be a slow operation. If some node fails on executing the operation,
     * it is retried for at most 3 times (on the failing nodes only). If it does not work after the third time, this
     * method throws a OPERATION_TIMEOUT back to the caller.
     *
     * @param name Name of the Replicated Map
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified for map add/remove/update/evict
     * events filtered by the given predicate.
     *
     * @param name Name of the Replicated Map
     * @param key Key with which the specified value is to be associated.
     * @param predicate The predicate for filtering entries
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate, boolean localOnly);

    /**
     * Adds an continuous entry listener for this map. The listener will be notified for map add/remove/update/evict
     * events filtered by the given predicate.
     *
     * @param name Name of the Replicated Map
     * @param predicate The predicate for filtering entries
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerWithPredicate(String name, Data predicate, boolean localOnly);

    /**
     * Adds the specified entry listener for the specified key. The listener will be notified for all
     * add/remove/update/evict events of the specified key only.
     *
     * @param name Name of the Replicated Map
     * @param key Key with which the specified value is to be associated.
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerToKey(String name, Data key, boolean localOnly);

    /**
     * Adds an entry listener for this map. The listener will be notified for all map add/remove/update/evict events.
     *
     * @param name Name of the ReplicatedMap
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListener(String name, boolean localOnly);

    /**
     * Removes the specified entry listener. Returns silently if there was no such listener added before.
     *
     * @param name Name of the ReplicatedMap
     * @param registrationId ID of the registered entry listener.
     * @return True if registration is removed, false otherwise.
     */
    @Request(id = 14, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     * Returns a lazy Set view of the key contained in this map. A LazySet is optimized for querying speed
     * (preventing eager deserialization and hashing on HashSet insertion) and does NOT provide all operations.
     * Any kind of mutating function will throw an UNSUPPORTED_OPERATION. Same is true for operations
     * like java.util.Set#contains(Object) and java.util.Set#containsAll(java.util.Collection) which would result in
     * very poor performance if called repeatedly (for example, in a loop). If the use case is different from querying
     * the data, please copy the resulting set into a new java.util.HashSet.
     *
     * @param name Name of the ReplicatedMap
     * @return A lazy set view of the keys contained in this map.
     */
    @Request(id = 15, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "random")
    Object keySet(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A collection view of the values contained in this map.
     */
    @Request(id = 16, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "random")
    Object values(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A lazy set view of the mappings contained in this map.
     */
    @Request(id = 17, retryable = true, response = ResponseMessageConst.LIST_ENTRY, partitionIdentifier = "random")
    Object entrySet(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param includeValue True if EntryEvent should contain the value,false otherwise
     * @param localOnly if true fires events that originated from this node only, otherwise fires all events
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addNearCacheEntryListener(String name, boolean includeValue, boolean localOnly);

}

