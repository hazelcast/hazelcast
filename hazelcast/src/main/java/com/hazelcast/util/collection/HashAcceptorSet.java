/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

import java.util.Set;

/**
 * Interface extending {@link Set} to accept hash on certain methods. This
 * can be act as an optimisation by avoiding the call to the expensive
 * {@link Object#hashCode()} methods if the hashCode is already known on
 * the caller side;
 *
 * @param <E> the type of elements maintained by this set
 */
public interface HashAcceptorSet<E> extends Set<E> {
    /**
     * Adds the specified element to this set if it is not already present.
     * <p>
     * This variant of {@link #add(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param elementToAdd element to be added to this set
     * @param hash         the hash of the element to be added
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     * @see #add(Object)
     */
    boolean add(E elementToAdd, int hash);

    /**
     * Returns <tt>true</tt> if this set contains the specified element
     * with the hash provided in parameter.
     * <p>
     * This variant of {@link #contains(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param objectToCheck element whose presence in this set is to be tested
     * @param hash          the hash of the element to be tested
     * @return <tt>true</tt> if this set contains the specified element
     * @see #contains(Object)
     */
    boolean contains(Object objectToCheck, int hash);

    /**
     * Removes the specified element from this set if it is present with
     * the hash provided in parameter.
     * <p>
     * This variant of {@link #remove(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param objectToRemove object to be removed from this set, if present
     * @param hash           the hash of the element to be removed
     * @return <tt>true</tt> if this set contained the specified element
     * @see #remove(Object)
     */
    boolean remove(Object objectToRemove, int hash);

    /**
     * Returns the current memory consumption (in bytes)
     *
     * @return the current memory consumption
     */
    long footprint();
}
