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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.impl.IndexImpl;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Map;

/**
 * Utils for accessing attribute entries.
 *
 */
public final class AttributeUtils {

    private AttributeUtils() {
    }

    static Comparable readAttribute(Map.Entry entry, String attribute) {
        QueryableEntry queryableEntry = (QueryableEntry) entry;
        Comparable value = queryableEntry.getAttribute(attribute);
        if (value == null) {
            return IndexImpl.NULL;
        }
        return value;
    }
}
