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
import java.util.Set;

@GenerateCodec(id = TemplateConstants.QUEUE_TEMPLATE_ID, name = "Queue", ns = "Hazelcast.Client.Protocol.Queue")
public interface QueueCodecTemplate {
    /**
     *
     * @param name Name of the Queue
     * @param value The element to add
     * @param timeoutMillis Maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return <tt>True</tt> if the element was added to this queue, else <tt>false</tt>
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object offer(String name, Data value, long timeoutMillis);

    /**
     *
     * @param name Name of the Queue
     * @param value The element to add
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID)
    void put(String name, Data value);

    /**
     *
     * @param name Name of the Queue
     * @return The number of elements in this collection
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     *
     * @param name Name of the Queue
     * @param value element to be removed from this queue, if present
     * @return <tt>true</tt> if this queue changed as a result of the call
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object remove(String name, Data value);

    /**
     *
     * @param name Name of the Queue
     * @param timeoutMillis Maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return The head of this queue, or <tt>null</tt> if this queue is empty
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.DATA)
    Object poll(String name, long timeoutMillis);

    /**
     *
     * @param name Name of the Queue
     * @return The head of this queue
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA)
    Object take(String name);

    /**
     *
     * @param name Name of the Queue
     * @return The head of this queue, or <tt>null</tt> if this queue is empty
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA)
    Object peek(String name);

    /**
     *
     * @param name Name of the Queue
     * @return list of all data in queue
     */

    @Request(id = 8, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object iterator(String name);

    /**
     *
     * @param name Name of the Queue
     * @return list of all removed data in queue
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object drainTo(String name);

    /**
     *
     * @param name Name of the Queue
     * @param maxSize The maximum number of elements to transfer
     * @return list of all removed data in result of this method
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object drainToMaxSize(String name, int maxSize);

    /**
     *
     * @param name Name of the Queue
     * @param value Element whose presence in this collection is to be tested
     * @return <tt>true</tt> if this collection contains the specified element
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object contains(String name, Data value);

    /**
     *
     * @param name Name of the Queue
     * @param dataList Collection to be checked for containment in this collection
     * @return <tt>true</tt> if this collection contains all of the elements in the specified collection
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object containsAll(String name, Set<Data> dataList);

    /**
     *
     * @param name Name of the Queue
     * @param dataList Collection containing elements to be removed from this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRemoveAll(String name, Set<Data> dataList);

    /**
     *
     * @param name Name of the Queue
     * @param dataList collection containing elements to be retained in this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRetainAll(String name, Set<Data> dataList);

    /**
     *
     * @param name Name of the Queue
     */
    @Request(id = 15, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     *
     * @param name Name of the Queue
     * @param dataList Collection containing elements to be added to this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object addAll(String name, List<Data> dataList);

    /**
     *
     * @param name Name of the Queue
     * @param includeValue <tt>true</tt> if the updated item should be passed to the item listener, <tt>false</tt> otherwise.
     * @return  The registration id
     */
    @Request(id = 17, retryable = true, response = ResponseMessageConst.STRING,
             event = {EventMessageConst.EVENT_ITEM})
    Object addListener(String name, boolean includeValue);

    /**
     *
     * @param name Name of the Queue
     * @param registrationId Id of the listener registration.
     * @return True if the item listener is removed, false otherwise
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeListener(String name, String registrationId);

    /**
     *
     * @param name Name of the Queue
     * @return The remaining capacity
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.INTEGER)
    Object remainingCapacity(String name);

    /**
     *
     * @param name Name of the Queue
     * @return <tt>True</tt> if this collection contains no elements
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

}
