/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

/*
* Written by Doug Lea with assistance from members of JCP JSR-166
* Expert Group and released to the public domain, as explained at
* http://creativecommons.org/licenses/publicdomain
*/

package com.hazelcast.util.concurrent;

import com.hazelcast.util.NavigableMap;
import com.hazelcast.util.NavigableSet;

import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link ConcurrentMap} supporting {@link NavigableMap} operations,
 * and recursively so for its navigable sub-maps.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @author Doug Lea
 */
public interface ConcurrentNavigableMap<K, V>
        extends ConcurrentMap<K, V>, NavigableMap<K, V> {
    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive,
                                        K toKey, boolean toInclusive);

    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive);

    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive);

    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey);

    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> headMap(K toKey);

    /**
     * @throws ClassCastException       {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    ConcurrentNavigableMap<K, V> tailMap(K fromKey);

    /**
     * Returns a reverse order view of the mappings contained in this map.
     * The descending map is backed by this map, so changes to the map are
     * reflected in the descending map, and vice-versa.
     * <p/>
     * <p>The returned map has an ordering equivalent to
     * <tt>{@link Collections#reverseOrder(Comparator) Collections.reverseOrder}(comparator())</tt>.
     * The expression {@code m.descendingMap().descendingMap()} returns a
     * view of {@code m} essentially equivalent to {@code m}.
     *
     * @return a reverse order view of this map
     */
    ConcurrentNavigableMap<K, V> descendingMap();

    /**
     * Returns a {@link NavigableSet} view of the keys contained in this map.
     * The set's iterator returns the keys in ascending order.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or {@code addAll}
     * operations.
     * <p/>
     * <p>The view's {@code iterator} is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     *
     * @return a navigable set view of the keys in this map
     */
    public NavigableSet<K> navigableKeySet();

    /**
     * Returns a {@link NavigableSet} view of the keys contained in this map.
     * The set's iterator returns the keys in ascending order.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or {@code addAll}
     * operations.
     * <p/>
     * <p>The view's {@code iterator} is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     * <p/>
     * <p>This method is equivalent to method {@code navigableKeySet}.
     *
     * @return a navigable set view of the keys in this map
     */
    NavigableSet<K> keySet();

    /**
     * Returns a reverse order {@link NavigableSet} view of the keys contained in this map.
     * The set's iterator returns the keys in descending order.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the {@code Iterator.remove}, {@code Set.remove},
     * {@code removeAll}, {@code retainAll}, and {@code clear}
     * operations.  It does not support the {@code add} or {@code addAll}
     * operations.
     * <p/>
     * <p>The view's {@code iterator} is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     *
     * @return a reverse order navigable set view of the keys in this map
     */
    public NavigableSet<K> descendingKeySet();
}