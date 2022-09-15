/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters;

/**
 * Base class for extraction of values from object instances.
 * Each subclass encapsulates extraction strategy.
 */
abstract class Getter {
    protected final Getter parent;

    Getter(final Getter parent) {
        this.parent = parent;
    }

    /**
     * Method for attribute-specific getters that can extract from one path only
     */
    abstract Object getValue(Object obj) throws Exception;

    /**
     * Method for generic getters that are not bound to a specific path only, but can get any attributePath
     * and extract from it.
     */
    Object getValue(Object obj, String attributePath) throws Exception {
        return getValue(obj);
    }

    /**
     * Method for generic getters that can make use of metadata if available. These getters must
     * gracefully fallback to not using metadata if unavailable.
     */
    Object getValue(Object obj, String attributePath, Object metadata) throws Exception {
        return getValue(obj, attributePath);
    }

    /**
     * Returns extracted object type for non-generic getters. It is only applicable when
     * extracted object type can be determined before running the getter.
     *
     * @return The type of extracted attribute
     */
    abstract Class getReturnType();

    /**
     * A getter instance may be re-used for all predicates that has the same target object
     * type and attribute path.
     *
     * Generic getters such as {@link PortableGetter} should not be cached because the same
     * getter is used for all entries regardless of attribute path. Instead, generic getters
     * should use a singleton instance.
     *
     * @return {@code true} if this getter is cacheable, {@code false} otherwise.
     */
    abstract boolean isCacheable();
}
