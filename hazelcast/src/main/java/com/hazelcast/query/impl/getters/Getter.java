/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Each sub-class encapsulates extraction strategy.
 */
abstract class Getter {
    protected final Getter parent;

    public Getter(final Getter parent) {
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

    abstract Class getReturnType();

    abstract boolean isCacheable();
}
