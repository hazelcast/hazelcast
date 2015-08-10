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

@GenerateCodec(id = TemplateConstants.LIST_TEMPLATE_ID, name = "List", ns = "Hazelcast.Client.Protocol.Codec")
public interface ListCodecTemplate {
    /**
     *
     * @param name Name of List
     * @return The number of elements in this list
     */
    @Request(id = 1, retryable = true, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     *
     * @param name Name of the List
     * @param value Element whose presence in this list is to be tested
     * @return True if this list contains the specified element, false otherwise
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object contains(String name, Data value);

    /**
     *
     * @param name Name of the List
     * @param valueSet Collection to be checked for containment in this list
     * @return True if this list contains all of the elements of the
     *         specified collection
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object containsAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the List
     * @param value Element to be appended to this list
     * @return true if this list changed as a result of the call, false otherwise
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object add(String name, Data value);

    /**
     *
     * @param name Name of the List
     * @param value Element to be removed from this list, if present
     * @return True if this list contained the specified element, false otherwise
     */

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object remove(String name, Data value);

    /**
     *
     * @param name Name of the List
     * @param valueList Collection containing elements to be added to this list
     * @return True if this list changed as a result of the call, false otherwise
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object addAll(String name, List<Data> valueList);

    /**
     *
     * @param name Name of the List
     * @param valueSet The list of values to compare for removal.
     * @return True if removed at least one of the items, false otherwise.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRemoveAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the List
     * @param valueSet The list of values to compare for retaining.
     * @return True if this list changed as a result of the call, false otherwise.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRetainAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the List
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    void clear(String name);

    /**
     *
     * @param name Name of the List
     * @return An array of all item values in the list.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object getAll(String name);

    /**
     *
     * @param name Name of the List
     * @param includeValue Set to true if you want the event to contain the value.
     * @return Registration id for the listener.
     */
    @Request(id = 11, retryable = true, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_ITEM})
    Object addListener(String name, boolean includeValue);

    /**
     *
     * @param name Name of the List
     * @param registrationId The id of the listener which was provided during registration.
     * @return True if unregistered, false otherwise.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeListener(String name, String registrationId);

    /**
     *
     * @param name Name of the List
     * @return True if this list contains no elements
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

    /**
     *
     * @param name Name of the List
     * @param index index at which to insert the first element from the
     *              specified collection
     * @param valueList The list of value to insert into the list.
     * @return True if this list changed as a result of the call, false otherwise.
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object addAllWithIndex(String name, int index, List<Data> valueList);

    /**
     *
     * @param name  Name of the List
     * @param index Index of the element to return
     * @return The element at the specified position in this list
     */
    @Request(id = 15, retryable = true, response = ResponseMessageConst.DATA)
    Object get(String name, int index);

    /**
     *
     * @param name Name of the List
     * @param index Index of the element to replace
     * @param value Element to be stored at the specified position
     * @return The element previously at the specified position
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.DATA)
    Object set(String name, int index, Data value);

    /**
     *
     * @param name Name of the List
     * @param index index at which the specified element is to be inserted
     * @param value Value to be inserted.
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.VOID)
    void addWithIndex(String name, int index, Data value);

    /**
     *
     * @param name Name of the List
     * @param index The index of the element to be removed
     * @return The element previously at the specified position
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.DATA)
    Object removeWithIndex(String name, int index);

    /**
     *
     * @param name Name of the List
     * @param value Element to search for
     * @return the index of the last occurrence of the specified element in
     *         this list, or -1 if this list does not contain the element
     */
    @Request(id = 19, retryable = true, response = ResponseMessageConst.INTEGER)
    Object lastIndexOf(String name, Data value);

    /**
     *
     * @param name Name of the List
     * @param value Element to search for
     * @return The index of the first occurrence of the specified element in
     *         this list, or -1 if this list does not contain the element
     */
    @Request(id = 20, retryable = true, response = ResponseMessageConst.INTEGER)
    Object indexOf(String name, Data value);

    /**
     *
     * @param name Name of the List
     * @param from Low endpoint (inclusive) of the subList
     * @param to High endpoint (exclusive) of the subList
     * @return A view of the specified range within this list
     */

    @Request(id = 21, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object sub(String name, int from, int to);

    /**
     *
     * @param name Name of the List
     * @return An iterator over the elements in this list in proper sequence
     */
    @Request(id = 22, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object iterator(String name);

    /**
     *
     * @param name Name of the List
     * @param index index of the first element to be returned from the
     *        list iterator next
     * @return a list iterator over the elements in this list (in proper
     *         sequence), starting at the specified position in the list
     */
    @Request(id = 23, retryable = true, response = ResponseMessageConst.LIST_DATA)
    Object listIterator(String name, int index);
}
