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

package com.hazelcast.internal.util;

/**
 * Mutable long which can be used for counting purposes.
 * <p>
 * This class is not thread-safe.
 */
public class MutableLong {

    /**
     * Mutable long value of this instance.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public long value;

    public static MutableLong valueOf(long value) {
        MutableLong instance = new MutableLong();
        instance.value = value;
        return instance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MutableLong that = (MutableLong) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "MutableLong{value=" + value + '}';
    }
}
