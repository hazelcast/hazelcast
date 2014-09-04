/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl;

import com.hazelcast.config.MultiMapConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Contains various {@link com.hazelcast.multimap.impl.MultiMapContainer} support methods.
 */
public abstract class AbstractMultiMapContainerSupport {

    /**
     * Picks right collection type, like {@link java.util.Set} or {@link java.util.List}
     * depending on the {@link com.hazelcast.config.MultiMapConfig#valueCollectionType} and creates it.
     *
     * @param collectionType one of {@link MultiMapConfig.ValueCollectionType#SET}
     *                       or {@link MultiMapConfig.ValueCollectionType#LIST}
     * @return {@link java.util.Set} or {@link java.util.List} depending on the collectionType argument
     * @throws java.lang.IllegalArgumentException
     */
    public static <T> Collection<T> createCollection(MultiMapConfig.ValueCollectionType collectionType) {
        return createCollection(collectionType, -1);
    }


    /**
     * Picks right collection type, like {@link java.util.Set} or {@link java.util.List}
     * depending on the {@link com.hazelcast.config.MultiMapConfig#valueCollectionType} and creates it.
     *
     * @param collectionType  one of {@link MultiMapConfig.ValueCollectionType#SET}
     *                        or {@link MultiMapConfig.ValueCollectionType#LIST}
     * @param initialCapacity if smaller than or equals to 0 falls back to default initial capacity of corresponding collection.
     * @return {@link java.util.Set} or {@link java.util.List} depending on the collectionType argument
     * @throws java.lang.IllegalArgumentException
     */
    public static <T> Collection<T> createCollection(MultiMapConfig.ValueCollectionType collectionType,
                                                     int initialCapacity) {
        switch (collectionType) {
            case SET:
                return initialCapacity <= 0 ? new HashSet<T>() : new HashSet<T>(initialCapacity);
            case LIST:
                return new LinkedList<T>();
            default:
                throw new IllegalArgumentException("[" + collectionType + "]"
                        + " is not a known MultiMapConfig.ValueCollectionType!");
        }
    }


    /**
     * Creates a same type collection with the given collection depending on the
     * {@link com.hazelcast.config.MultiMapConfig#valueCollectionType}.
     *
     * @param collection to be asked to return a new appropriate implementation instance
     *                   according to {@link MultiMapConfig.ValueCollectionType}
     * @return {@link java.util.Set} or {@link java.util.List} depending on the collectionType argument
     * @throws java.lang.IllegalArgumentException
     */
    public static <T> Collection<T> createCollection(Collection collection) {
        final MultiMapConfig.ValueCollectionType collectionType = findCollectionType(collection);
        if (collection == null || collection.isEmpty()) {
            return emptyCollection(collectionType);
        }
        return createCollection(collectionType, collection.size());
    }

    /**
     * Picks right empty collection type, like {@link java.util.Collections#emptySet()}
     * or {@link java.util.Collections#emptyList()} )} depending on the
     * {@link com.hazelcast.config.MultiMapConfig#valueCollectionType}
     *
     * @param collectionType one of {@link MultiMapConfig.ValueCollectionType#SET}
     *                       or {@link MultiMapConfig.ValueCollectionType#LIST}
     * @return empty collection like {@link java.util.Collections#emptySet()}
     * depending on the collectionType argument
     * @throws java.lang.IllegalArgumentException
     */
    public static <T> Collection<T> emptyCollection(MultiMapConfig.ValueCollectionType collectionType) {
        switch (collectionType) {
            case SET:
                return Collections.emptySet();
            case LIST:
                return Collections.emptyList();
            default:
                throw new IllegalArgumentException("[" + collectionType + "]"
                        + " is not a known MultiMapConfig.ValueCollectionType!");
        }
    }


    /**
     * Returns corresponding {@link MultiMapConfig.ValueCollectionType} of a {@link java.util.Collection}
     *
     * @param collection {@link MultiMapConfig.ValueCollectionType} to be find
     * @return corresponding {@link MultiMapConfig.ValueCollectionType} of a {@link java.util.Collection}
     * @throws java.lang.IllegalArgumentException
     */
    private static MultiMapConfig.ValueCollectionType findCollectionType(Collection collection) {
        if (collection instanceof Set) {
            return MultiMapConfig.ValueCollectionType.SET;
        } else if (collection instanceof List) {
            return MultiMapConfig.ValueCollectionType.LIST;
        }

        throw new IllegalArgumentException("[" + collection.getClass() + "]"
                + " is not a known MultiMapConfig.ValueCollectionType!");
    }
}
