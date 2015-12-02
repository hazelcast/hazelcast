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

@GenerateCodec(id = TemplateConstants.QUEUE_TEMPLATE_ID, name = "Queue", ns = "Hazelcast.Client.Protocol.Codec")
public interface QueueCodecTemplate {
    /**
     * Inserts the specified element into this queue, waiting up to the specified wait time if necessary for space to
     * become available.
     *
     * @param name          Name of the Queue
     * @param value         The element to add
     * @param timeoutMillis Maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return <tt>True</tt> if the element was added to this queue, else <tt>false</tt>
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object offer(String name, Data value, long timeoutMillis);

    /**
     * Inserts the specified element into this queue, waiting if necessary for space to become available.
     *
     * @param name  Name of the Queue
     * @param value The element to add
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void put(String name, Data value);

    /**
     * Returns the number of elements in this collection.  If this collection contains more than Integer.MAX_VALUE
     * elements, returns Integer.MAX_VALUE
     *
     * @param name Name of the Queue
     * @return The number of elements in this collection
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object size(String name);

    /**
     * Retrieves and removes the head of this queue.  This method differs from poll only in that it throws an exception
     * if this queue is empty.
     *
     * @param name  Name of the Queue
     * @param value Element to be removed from this queue, if present
     * @return <tt>true</tt> if this queue changed as a result of the call
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object remove(String name, Data value);

    /**
     * Retrieves and removes the head of this queue, waiting up to the specified wait time if necessary for an element
     * to become available.
     *
     * @param name          Name of the Queue
     * @param timeoutMillis Maximum time in milliseconds to wait for acquiring the lock for the key.
     * @return The head of this queue, or <tt>null</tt> if this queue is empty
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object poll(String name, long timeoutMillis);

    /**
     * Retrieves and removes the head of this queue, waiting if necessary until an element becomes available.
     *
     * @param name Name of the Queue
     * @return The head of this queue
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object take(String name);

    /**
     * Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
     *
     * @param name Name of the Queue
     * @return The head of this queue, or <tt>null</tt> if this queue is empty
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object peek(String name);

    /**
     * Returns an iterator over the elements in this collection.  There are no guarantees concerning the order in which
     * the elements are returned (unless this collection is an instance of some class that provides a guarantee).
     *
     * @param name Name of the Queue
     * @return list of all data in queue
     */

    @Request(id = 8, retryable = false, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object iterator(String name);

    /**
     * Removes all available elements from this queue and adds them to the given collection.  This operation may be more
     * efficient than repeatedly polling this queue.  A failure encountered while attempting to add elements to
     * collection c may result in elements being in neither, either or both collections when the associated exception is
     * thrown. Attempts to drain a queue to itself result in ILLEGAL_ARGUMENT. Further, the behavior of
     * this operation is undefined if the specified collection is modified while the operation is in progress.
     *
     * @param name Name of the Queue
     * @return list of all removed data in queue
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object drainTo(String name);

    /**
     * Removes at most the given number of available elements from this queue and adds them to the given collection.
     * A failure encountered while attempting to add elements to collection may result in elements being in neither,
     * either or both collections when the associated exception is thrown. Attempts to drain a queue to itself result in
     * ILLEGAL_ARGUMENT. Further, the behavior of this operation is undefined if the specified collection is
     * modified while the operation is in progress.
     *
     * @param name    Name of the Queue
     * @param maxSize The maximum number of elements to transfer
     * @return list of all removed data in result of this method
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object drainToMaxSize(String name, int maxSize);

    /**
     * Returns true if this queue contains the specified element. More formally, returns true if and only if this queue
     * contains at least one element e such that value.equals(e)
     *
     * @param name  Name of the Queue
     * @param value Element whose presence in this collection is to be tested
     * @return <tt>true</tt> if this collection contains the specified element
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object contains(String name, Data value);

    /**
     * Return true if this collection contains all of the elements in the specified collection.
     *
     * @param name     Name of the Queue
     * @param dataList Collection to be checked for containment in this collection
     * @return <tt>true</tt> if this collection contains all of the elements in the specified collection
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object containsAll(String name, List<Data> dataList);

    /**
     * Removes all of this collection's elements that are also contained in the specified collection (optional operation).
     * After this call returns, this collection will contain no elements in common with the specified collection.
     *
     * @param name     Name of the Queue
     * @param dataList Collection containing elements to be removed from this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object compareAndRemoveAll(String name, List<Data> dataList);

    /**
     * Retains only the elements in this collection that are contained in the specified collection (optional operation).
     * In other words, removes from this collection all of its elements that are not contained in the specified collection.
     *
     * @param name     Name of the Queue
     * @param dataList collection containing elements to be retained in this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object compareAndRetainAll(String name, List<Data> dataList);

    /**
     * Removes all of the elements from this collection (optional operation). The collection will be empty after this
     * method returns.
     *
     * @param name Name of the Queue
     */
    @Request(id = 15, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void clear(String name);

    /**
     * Adds all of the elements in the specified collection to this collection (optional operation).The behavior of this
     * operation is undefined if the specified collection is modified while the operation is in progress.
     * (This implies that the behavior of this call is undefined if the specified collection is this collection,
     * and this collection is nonempty.)
     *
     * @param name     Name of the Queue
     * @param dataList Collection containing elements to be added to this collection
     * @return <tt>true</tt> if this collection changed as a result of the call
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object addAll(String name, List<Data> dataList);

    /**
     * Adds an listener for this collection. Listener will be notified or all collection add/remove events.
     *
     * @param name         Name of the Queue
     * @param includeValue <tt>true</tt> if the updated item should be passed to the item listener, <tt>false</tt> otherwise.
     * @param localOnly    if true fires events that originated from this node only, otherwise fires all events
     * @return The registration id
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.STRING,
            event = {EventMessageConst.EVENT_ITEM})
    Object addListener(String name, boolean includeValue, boolean localOnly);

    /**
     * Removes the specified item listener.Returns silently if the specified listener was not added before.
     *
     * @param name           Name of the Queue
     * @param registrationId Id of the listener registration.
     * @return True if the item listener is removed, false otherwise
     */
    @Request(id = 18, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object removeListener(String name, String registrationId);

    /**
     * Returns the number of additional elements that this queue can ideally (in the absence of memory or resource
     * constraints) accept without blocking, or Integer.MAX_VALUE if there is no intrinsic limit. Note that you cannot
     * always tell if an attempt to insert an element will succeed by inspecting remainingCapacity because it may be
     * the case that another thread is about to insert or remove an element.
     *
     * @param name Name of the Queue
     * @return The remaining capacity
     */
    @Request(id = 19, retryable = false, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object remainingCapacity(String name);

    /**
     * Returns true if this collection contains no elements.
     *
     * @param name Name of the Queue
     * @return <tt>True</tt> if this collection contains no elements
     */
    @Request(id = 20, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object isEmpty(String name);

}
