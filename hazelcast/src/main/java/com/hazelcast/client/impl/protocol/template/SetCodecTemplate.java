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

@GenerateCodec(id = TemplateConstants.SET_TEMPLATE_ID, name = "Set", ns = "Hazelcast.Client.Protocol.Set")
public interface SetCodecTemplate {
    /**
     *
     * @param name Name of the Set
     * @return The number of elements in this set (its cardinality)
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.INTEGER)
    Object size(String name);

    /**
     *
     * @param name Name of the Set
     * @param value Element whose presence in this set is to be tested
     * @return True if this set contains the specified element, false otherwise
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object contains(String name, Data value);

    /**
     *
     * @param name Name of the Set
     * @param valueSet Collection to be checked for containment in this set
     * @return true if this set contains all of the elements of the
     *         specified collection
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object containsAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the Set
     * @param value Element to be added to this set
     * @return True if this set did not already contain the specified
     *         element and the element is added, returns false otherwise.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object add(String name, Data value);

    /**
     *
     * @param name Name of the Set
     * @param value Object to be removed from this set, if present
     * @return True if this set contained the specified element and it is removed successfully
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object remove(String name, Data value);

    /**
     *
     * @param name Name of the Set
     * @param valueList Collection containing elements to be added to this set
     * @return True if this set changed as a result of the call
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object addAll(String name, List<Data> valueList);

    /**
     *
     * @param name Name of the Set
     * @param valueSet The set of values to test for matching the item to remove.
     * @return true if at least one item in valueSet existed and removed, false otherwise.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRemoveAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the Set
     * @param valueSet The set of values to test for matching the item to retain.
     * @return true if at least one item in valueSet existed and it is retained, false otherwise. All items not in valueSet but
     *        in the Set are removed.
     */
    @Request(id = 8, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object compareAndRetainAll(String name, Set<Data> valueSet);

    /**
     *
     * @param name Name of the Set
     */
    @Request(id = 9, retryable = false, response = ResponseMessageConst.VOID)
    Object clear(String name);

    /**
     *
     * @param name Name of the Set
     * @return Array of all values in the Set
     */
    @Request(id = 10, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object getAll(String name);

    /**
     *
     * @param name Name of the Set
     * @param includeValue if set to true, the event shall also include the value.
     * @return The registration id.
     */
    @Request(id = 11, retryable = true, response = ResponseMessageConst.STRING,
    event = {EventMessageConst.EVENT_ITEM})
    Object addListener(String name, boolean includeValue);

    /**
     *
     * @param name Name of the Set
     * @param registrationId The id retrieved during registration.
     * @return true if the listener with the provided id existed and removed, false otherwise.
     */
    @Request(id = 12, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object removeListener(String name, String registrationId);
    
    /**
     *
     * @param name Name of the Set
     * @return True if this set contains no elements
     */
    @Request(id = 13, retryable = false, response = ResponseMessageConst.BOOLEAN)
    Object isEmpty(String name);

}
