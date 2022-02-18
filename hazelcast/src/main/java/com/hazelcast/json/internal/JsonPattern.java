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

package com.hazelcast.json.internal;

import java.util.ArrayList;
import java.util.List;

/**
 * A JsonPattern is a structure that represents a logical path to a
 * terminal value in a Json object. A logical path is a series of numbers
 * that defines the location of a specific item within their parent.
 * If an item is within an object, than the corresponding number is the
 * order of that item within the object. If an item is within an array,
 * then the corresponding number is the index of that item within the
 * array.
 *
 * For example;
 * A Json object is given:
 * {
 *     "attr1": 10,
 *     "attr2": [
 *          "aText",
 *          "anotherText"
 *     ]
 * }
 * The path "attr2[1]" represents "anotherText" JsonString. The logical
 * position of this value is "1.1" because "attr2" is the second attribute
 * and we are looking for the second item in the corresponding array.
 */
public class JsonPattern {

    private final List<Integer> pattern;
    private boolean containsAny;

    /**
     * Creates an empty JsonPattern.
     */
    public JsonPattern() {
        this(new ArrayList<Integer>());
    }

    public JsonPattern(List<Integer> list) {
        pattern = list;
    }

    /**
     * Creates a deep copy of a JsonPattern
     * @param other
     */
    public JsonPattern(JsonPattern other) {
        this(new ArrayList<Integer>(other.pattern));
    }

    public int get(int index) {
        return pattern.get(index);
    }

    public void add(int patternItem) {
        pattern.add(patternItem);
    }

    /**
     * Marks this pattern as having "any" keyword. It is upto the user
     * how to use this information.
     * See {@link #hasAny()}
     */
    public void addAny() {
        containsAny = true;
    }

    public void add(JsonPattern other) {
        pattern.addAll(other.pattern);
    }

    /**
     * Checks if this pattern has "any" keyword.
     *
     * @return {@code true} if this pattern has "any"
     */
    public boolean hasAny() {
        return containsAny;
    }

    /**
     * Returns the depth of this pattern.
     *
     * @return the depth of this pattern
     */
    public int depth() {
        return pattern.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JsonPattern pattern1 = (JsonPattern) o;

        if (containsAny != pattern1.containsAny) {
            return false;
        }
        return pattern != null ? pattern.equals(pattern1.pattern) : pattern1.pattern == null;
    }

    @Override
    public int hashCode() {
        int result = pattern != null ? pattern.hashCode() : 0;
        result = 31 * result + (containsAny ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JsonPattern{"
                + "pattern=" + pattern
                + ", containsAny=" + containsAny
                + '}';
    }
}
