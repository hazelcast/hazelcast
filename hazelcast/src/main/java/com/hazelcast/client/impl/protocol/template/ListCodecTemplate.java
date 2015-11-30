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

@GenerateCodec(id = TemplateConstants.LIST_TEMPLATE_ID, name = "List", ns = "Hazelcast.Client.Protocol.Codec")
public interface ListCodecTemplate {
    /**
     * Returns the number of elements in this list.  If this list contains more than Integer.MAX_VALUE elements, returns
     * Integer.MAX_VALUE.
     *
     * @param name Name of List
     * @return The number of elements in this list
     */
    @Request(id = 1, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object size(String name);

    /**
     * Returns true if this list contains the specified element.
     *
     * @param name  Name of the List
     * @param value Element whose presence in this list is to be tested
     * @return True if this list contains the specified element, false otherwise
     */
    @Request(id = 2, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object contains(String name, Data value);

    /**
     * Returns true if this list contains all of the elements of the specified collection.
     *
     * @param name   Name of the List
     * @param values Collection to be checked for containment in this list
     * @return True if this list contains all of the elements of the
     * specified collection
     */
    @Request(id = 3, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object containsAll(String name, List<Data> values);

    /**
     * Appends the specified element to the end of this list (optional operation). Lists that support this operation may
     * place limitations on what elements may be added to this list.  In particular, some lists will refuse to add null
     * elements, and others will impose restrictions on the type of elements that may be added. List classes should
     * clearly specify in their documentation any restrictions on what elements may be added.
     *
     * @param name  Name of the List
     * @param value Element to be appended to this list
     * @return true if this list changed as a result of the call, false otherwise
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object add(String name, Data value);

    /**
     * Removes the first occurrence of the specified element from this list, if it is present (optional operation).
     * If this list does not contain the element, it is unchanged.
     * Returns true if this list contained the specified element (or equivalently, if this list changed as a result of the call).
     *
     * @param name  Name of the List
     * @param value Element to be removed from this list, if present
     * @return True if this list contained the specified element, false otherwise
     */

    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object remove(String name, Data value);

    /**
     * Appends all of the elements in the specified collection to the end of this list, in the order that they are
     * returned by the specified collection's iterator (optional operation).
     * The behavior of this operation is undefined if the specified collection is modified while the operation is in progress.
     * (Note that this will occur if the specified collection is this list, and it's nonempty.)
     *
     * @param name      Name of the List
     * @param valueList Collection containing elements to be added to this list
     * @return True if this list changed as a result of the call, false otherwise
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object addAll(String name, List<Data> valueList);

    /**
     * Removes from this list all of its elements that are contained in the specified collection (optional operation).
     *
     * @param name   Name of the List
     * @param values The list of values to compare for removal.
     * @return True if removed at least one of the items, false otherwise.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object compareAndRemoveAll(String name, List<Data> values);

    /**
     * Retains only the elements in this list that are contained in the specified collection (optional operation).
     * In other words, removes from this list all of its elements that are not contained in the specified collection.
     *
     * @param name   Name of the List
     * @param values The list of values to compare for retaining.
     * @return True if this list changed as a result of the call, false otherwise.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object compareAndRetainAll(String name, List<Data> values);

    /**
     * Removes all of the elements from this list (optional operation). The list will be empty after this call returns.
     *
     * @param name Name of the List
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void clear(String name);

    /**
     * Return the all elements of this collection
     *
     * @param name Name of the List
     * @return An array of all item values in the list.
     */
    @Request(id = 10, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object getAll(String name);

    /**
     * Adds an item listener for this collection. Listener will be notified for all collection add/remove events.
     *
     * @param name         Name of the List
     * @param includeValue Set to true if you want the event to contain the value.
     * @param localOnly    if true fires events that originated from this node only, otherwise fires all events
     * @return Registration id for the listener.
     */
    @Request(id = 11, retryable = false, response = ResponseMessageConst.STRING, event = {EventMessageConst.EVENT_ITEM}, partitionIdentifier = "name")
    Object addListener(String name, boolean includeValue, boolean localOnly);

    /**
     * Removes the specified item listener. Returns silently if the specified listener was not added before.
     *
     * @param name           Name of the List
     * @param registrationId The id of the listener which was provided during registration.
     * @return True if unregistered, false otherwise.
     */
    @Request(id = 12, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object removeListener(String name, String registrationId);

    /**
     * Returns true if this list contains no elements
     *
     * @param name Name of the List
     * @return True if this list contains no elements
     */
    @Request(id = 13, retryable = true, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object isEmpty(String name);

    /**
     * Inserts all of the elements in the specified collection into this list at the specified position (optional operation).
     * Shifts the element currently at that position (if any) and any subsequent elements to the right (increases their indices).
     * The new elements will appear in this list in the order that they are returned by the specified collection's iterator.
     * The behavior of this operation is undefined if the specified collection is modified while the operation is in progress.
     * (Note that this will occur if the specified collection is this list, and it's nonempty.)
     *
     * @param name      Name of the List
     * @param index     index at which to insert the first element from the specified collection.
     * @param valueList The list of value to insert into the list.
     * @return True if this list changed as a result of the call, false otherwise.
     */
    @Request(id = 14, retryable = false, response = ResponseMessageConst.BOOLEAN, partitionIdentifier = "name")
    Object addAllWithIndex(String name, int index, List<Data> valueList);

    /**
     * Returns the element at the specified position in this list
     *
     * @param name  Name of the List
     * @param index Index of the element to return
     * @return The element at the specified position in this list
     */
    @Request(id = 15, retryable = true, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object get(String name, int index);

    /**
     * The element previously at the specified position
     *
     * @param name  Name of the List
     * @param index Index of the element to replace
     * @param value Element to be stored at the specified position
     * @return The element previously at the specified position
     */
    @Request(id = 16, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object set(String name, int index, Data value);

    /**
     * Inserts the specified element at the specified position in this list (optional operation). Shifts the element
     * currently at that position (if any) and any subsequent elements to the right (adds one to their indices).
     *
     * @param name  Name of the List
     * @param index index at which the specified element is to be inserted
     * @param value Value to be inserted.
     */
    @Request(id = 17, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "name")
    void addWithIndex(String name, int index, Data value);

    /**
     * Removes the element at the specified position in this list (optional operation). Shifts any subsequent elements
     * to the left (subtracts one from their indices). Returns the element that was removed from the list.
     *
     * @param name  Name of the List
     * @param index The index of the element to be removed
     * @return The element previously at the specified position
     */
    @Request(id = 18, retryable = false, response = ResponseMessageConst.DATA, partitionIdentifier = "name")
    Object removeWithIndex(String name, int index);

    /**
     * Returns the index of the last occurrence of the specified element in this list, or -1 if this list does not
     * contain the element.
     *
     * @param name  Name of the List
     * @param value Element to search for
     * @return the index of the last occurrence of the specified element in
     * this list, or -1 if this list does not contain the element
     */
    @Request(id = 19, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object lastIndexOf(String name, Data value);

    /**
     * Returns the index of the first occurrence of the specified element in this list, or -1 if this list does not
     * contain the element.
     *
     * @param name  Name of the List
     * @param value Element to search for
     * @return The index of the first occurrence of the specified element in
     * this list, or -1 if this list does not contain the element
     */
    @Request(id = 20, retryable = true, response = ResponseMessageConst.INTEGER, partitionIdentifier = "name")
    Object indexOf(String name, Data value);

    /**
     * Returns a view of the portion of this list between the specified from, inclusive, and to, exclusive.(If from and
     * to are equal, the returned list is empty.) The returned list is backed by this list, so non-structural changes in
     * the returned list are reflected in this list, and vice-versa. The returned list supports all of the optional list
     * operations supported by this list.
     * This method eliminates the need for explicit range operations (of the sort that commonly exist for arrays).
     * Any operation that expects a list can be used as a range operation by passing a subList view instead of a whole list.
     * Similar idioms may be constructed for indexOf and lastIndexOf, and all of the algorithms in the Collections class
     * can be applied to a subList.
     * The semantics of the list returned by this method become undefined if the backing list (i.e., this list) is
     * structurally modified in any way other than via the returned list.(Structural modifications are those that change
     * the size of this list, or otherwise perturb it in such a fashion that iterations in progress may yield incorrect results.)
     *
     * @param name Name of the List
     * @param from Low endpoint (inclusive) of the subList
     * @param to   High endpoint (exclusive) of the subList
     * @return A view of the specified range within this list
     */

    @Request(id = 21, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object sub(String name, int from, int to);

    /**
     * Returns an iterator over the elements in this list in proper sequence.
     *
     * @param name Name of the List
     * @return An iterator over the elements in this list in proper sequence
     */
    @Request(id = 22, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object iterator(String name);

    /**
     * Returns a list iterator over the elements in this list (in proper sequence), starting at the specified position
     * in the list. The specified index indicates the first element that would be returned by an initial call to
     * ListIterator#next next. An initial call to ListIterator#previous previous would return the element with the
     * specified index minus one.
     *
     * @param name  Name of the List
     * @param index index of the first element to be returned from the list iterator next
     * @return a list iterator over the elements in this list (in proper
     * sequence), starting at the specified position in the list
     */
    @Request(id = 23, retryable = true, response = ResponseMessageConst.LIST_DATA, partitionIdentifier = "name")
    Object listIterator(String name, int index);
}
