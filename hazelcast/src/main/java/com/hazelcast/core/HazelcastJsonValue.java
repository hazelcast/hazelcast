/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * HazelcastJsonValue is a wrapper for JSON formatted strings. It is preferred
 * to store HazelcastJsonValue instead of Strings for JSON-formatted strings.
 * It allows you to run predicates/aggregations and use indexes on the
 * attributes of the underlying JSON strings.
 *
 * <p>HazelcastJsonValue can be queried for fields in Hazelcast's queries. See
 * {@link com.hazelcast.query.Predicates}.
 *
 * <p>When querying, numbers in JSON strings are treated as either
 * {@code Long} or {@code Double}. Strings, booleans and nulls are
 * treated as their Java counterparts.
 *
 * <p>HazelcastJsonValue stores the given string as is. It is not validated.
 * Ill-formatted JSON strings may cause false positive or false negative
 * results in queries. {@code null} string is not allowed.
 */
public final class HazelcastJsonValue {

    private final String string;

    /**
     * Creates a HazelcastJsonValue from the given string.
     *
     * @param string a non-null JSON string
     */
    public HazelcastJsonValue(String string) {
        this.string = checkNotNull(string);
    }

    /**
     * Returns unaltered string that was used to create this object.
     * @return original string
     */
    @Override
    public String toString() {
        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HazelcastJsonValue that = (HazelcastJsonValue) o;
        return string.equals(that.string);
    }

    @Override
    public int hashCode() {
        return string.hashCode();
    }
}
