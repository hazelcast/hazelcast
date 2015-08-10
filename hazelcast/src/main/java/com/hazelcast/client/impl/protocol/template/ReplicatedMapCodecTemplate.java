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

@GenerateCodec(id = TemplateConstants.REPLICATED_MAP_TEMPLATE_ID,
        name = "ReplicatedMap", ns = "Hazelcast.Client.Protocol.Codec")
public interface ReplicatedMapCodecTemplate {
    /**
     *
     * @param name Name of the ReplicatedMap
     * @param key  Key with which the specified value is to be associated.
     * @param value Value to be associated with the specified key
     * @param ttl ttl in milliseconds to be associated with the specified key-value pair
     * @return The old value if existed for the key.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.DATA)
    Object put(String name, Data key, Data value, long ttl);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return the number of key-value mappings in this map.
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return <tt>True</tt> if this map contains no key-value mappings
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param key The key whose associated value is to be returned.
     * @return <tt>True</tt> if this map contains a mapping for the specified key
     *
     */
    @Request(id = 4, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsKey(String name, Data key);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the specified value
     */
    @Request(id = 5, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsValue(String name, Data value);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param key The key whose associated value is to be returned
     * @return The value to which the specified key is mapped, or null if this map contains no mapping for the key
     */
    @Request(id = 6, retryable = true, response = ResponseMessageConst.DATA)
    Object get(String name, Data key);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param key Key with which the specified value is to be associated.
     * @return the previous value associated with <tt>key</tt>, or <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA)
    Object remove(String name, Data key);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param map Mappings to be stored in this map
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.VOID)
    void putAll(String name, Map<Data, Data> map);

    /**
     *
     * @param name Name of the Replicated Map
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     *
     * @param name Name of the Replicated Map
     * @param key Key with which the specified value is to be associated.
     * @param predicate The predicate for filtering entries
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerToKeyWithPredicate(String name, Data key, Data predicate);

    /**
     *
     * @param name Name of the Replicated Map
     * @param predicate The predicate for filtering entries
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 11, retryable = true, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerWithPredicate(String name, Data predicate);

    /**
     *
     * @param name Name of the Replicated Map
     * @param key Key with which the specified value is to be associated.
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 12, retryable = true, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListenerToKey(String name, Data key);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.STRING
            , event = {EventMessageConst.EVENT_ENTRY})
    Object addEntryListener(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param registrationId ID of the registered entry listener.
     * @return True if registration is removed, false otherwise.
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeEntryListener(String name, String registrationId);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A lazy set view of the keys contained in this map.
     */
    @Request(id = 15, retryable = true, response = ResponseMessageConst.SET_DATA)
    Object keySet(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A collection view of the values contained in this map.
     */
    @Request(id = 16, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object values(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @return A lazy set view of the mappings contained in this map.
     */
    @Request(id = 17, retryable = true, response = ResponseMessageConst.SET_ENTRY)
    Object entrySet(String name);

    /**
     *
     * @param name Name of the ReplicatedMap
     * @param includeValue True if EntryEvent should contain the value,false otherwise
     * @return A unique string  which is used as a key to remove the listener.
     */
    @Request(id = 18, retryable = true, response = ResponseMessageConst.STRING, event = EventMessageConst.EVENT_ENTRY)
    Object addNearCacheEntryListener(String name, boolean includeValue);

}

